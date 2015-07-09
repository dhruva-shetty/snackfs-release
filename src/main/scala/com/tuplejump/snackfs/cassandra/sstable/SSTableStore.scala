package com.tuplejump.snackfs.cassandra.sstable

import java.io.File
import java.io.FilenameFilter
import java.nio.file.FileSystems
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.attribute.BasicFileAttributes
import java.util.List
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaConcurrentMap
import scala.collection.mutable.Map
import scala.compat.Platform

import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.marshal.BytesType
import org.apache.cassandra.io.sstable.Descriptor
import org.apache.cassandra.io.sstable.SSTableReader

import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger

object SSTableStore {

  private lazy val log = Logger.get(getClass)

  val service: ExecutorService = new ThreadPoolExecutor(2, 5, 1, TimeUnit.MINUTES, new LinkedBlockingQueue)
  
  val watcher = FileSystems.getDefault.newWatchService
  
  val watchedEventsByDirectory = new ConcurrentHashMap[WatchKey, Path]
  
  val ssTableMap: Map[String, SSTableReader] = new ConcurrentHashMap[String, SSTableReader]
  
  val metadata: Map[String, CFMetaData] = new ConcurrentHashMap[String, CFMetaData]
  
  @volatile var initialized = false
  
  @volatile var sblockDirectory: File = null

  def getMetaData(keyspace: String, columnFamily: String): CFMetaData = {
    if (!metadata.keySet.contains(columnFamily)) {
      metadata.put(columnFamily, CFMetaData.denseCFMetaData(keyspace, columnFamily, BytesType.instance))
    }
    metadata.get(columnFamily).head
  }

  def getSSTables(keyspace: String, directoryPath: String, columnFamily: String): Map[String, SSTableReader] = {
    val start = Platform.currentTime
    intializeSSTableDirectory(directoryPath, columnFamily)
    if (sblockDirectory != null && !initialized) {
      this.synchronized({
        if (!initialized) {
          initDirectoryWatch(FileSystems.getDefault.getPath(sblockDirectory.getAbsolutePath), keyspace, columnFamily)
          var files = sblockDirectory.listFiles(new FilenameFilter {
            override def accept(dir: File, name: String): Boolean = return name.endsWith("Data.db") && !name.contains("tmp")
          })
          var filesRead = new AtomicInteger(0)
          files.par.foreach { file =>
            if (!ssTableMap.keySet.contains(file.getAbsolutePath)) {
              try {
                if (LogConfiguration.isDebugEnabled) log.debug("Reading file: %s/%s", sblockDirectory.getAbsolutePath, file.getName)
                ssTableMap.put(file.getAbsolutePath, SSTableReader.open(Descriptor.fromFilename(new File(sblockDirectory.getAbsolutePath), file.getName).left, getMetaData(keyspace, columnFamily)))
                filesRead.incrementAndGet
              } catch {
                case e: Throwable => log.error(e, "Failed to read sstable %s", file.getName)
              }
            }
          }
          if (filesRead != 0) {
            log.info("Elapsed time to load %s sstables: %s ms", filesRead, (Platform.currentTime - start))
          }
          initialized = true
        }
      })
    }
    if (ssTableMap.isEmpty) {
      log.error("No sstables were found for: %s", sblockDirectory)
      return null
    }
    ssTableMap
  }

  def intializeSSTableDirectory(directoryPath: String, columnFamily: String) = {
    if (sblockDirectory == null) {
      this.synchronized({
        if (sblockDirectory == null && !Files.exists(Paths.get(directoryPath + columnFamily))) {
          val directory = new File(directoryPath).listFiles(new FilenameFilter {
            override def accept(dir: File, name: String): Boolean = return name.startsWith(columnFamily)
          })
          if (directory != null && directory.length > 0) {
            sblockDirectory = directory.head
          }
        }
      })
    }
  }

  def initDirectoryWatch(dirPath: Path, keyspace: String, columnFamily: String) {
    Files.walkFileTree(dirPath, new SimpleFileVisitor[Path] {
      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
        val watchKey = dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE)
        if (watchKey != null) {
          watchedEventsByDirectory.put(watchKey, dir)
          log.info("Watching directory: %s", dir)
        }
        return FileVisitResult.CONTINUE
      }
    })
    service.submit(new Runnable {
      override def run {
        while (true) {
          var watchKey: WatchKey = null
          try {
            watchKey = watcher.take
            val dir = watchedEventsByDirectory.get(watchKey)
            if (dir != null) {
              val events = watchKey.pollEvents
            	log.info("Handling %s event(s) for %s", events.size, dir)
              handleEvent(dir, events)
            }
            if (!watchKey.reset) {
              log.error("Failed to re-register watchKey for: %s", dir)
              watchedEventsByDirectory.remove(watchKey);
              if (watchedEventsByDirectory.isEmpty) {
                return
              }
            }
          } catch {
            case ex: InterruptedException => {
              log.error(ex, "Failed to add watch!")
            }
          }
        }
      }

      def handleEvent(dir: Path, events: List[WatchEvent[_]]) = {
        service.submit(new Runnable {
          override def run {
            events.foreach { event =>
              {
            	  val kind = event.kind
                val filePath = dir.resolve(event.asInstanceOf[WatchEvent[Path]].context)
                log.info("Handling event: " + kind.name + " path: " + filePath)
                if (kind == StandardWatchEventKinds.ENTRY_CREATE || kind == StandardWatchEventKinds.ENTRY_DELETE) {
                  val file = filePath.toFile
                  if (!Files.isDirectory(filePath, LinkOption.NOFOLLOW_LINKS) && isFileOfInterest(file.getName)) {
                    kind match {
                    case StandardWatchEventKinds.ENTRY_CREATE =>
                      if (!ssTableMap.contains(file.getAbsolutePath)) {
                        val start = Platform.currentTime
                        ssTableMap.put(file.getAbsolutePath, SSTableReader.open(Descriptor.fromFilename(new File(sblockDirectory.getAbsolutePath), file.getName).left, getMetaData(keyspace, columnFamily)))
                        log.info("Elaped time to add sstable %s is %s ms", file.getAbsolutePath, (Platform.currentTime - start))
                      }
                    case StandardWatchEventKinds.ENTRY_DELETE =>
                      if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                    	  log.info("Removing sstable %s ", file)
                        ssTableMap.remove(file.getAbsolutePath)
                      }
                    case default =>
                      log.info("Ignoring event %s for %s ", default, file)
                    }
                  }
                }
              }
            }
          }
        })
      }

      private def isFileOfInterest(fileName: String) = fileName != null && fileName.endsWith("Data.db") && !fileName.contains("tmp")
      
    })
  }
}