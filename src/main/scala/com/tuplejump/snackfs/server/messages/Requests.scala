package com.tuplejump.snackfs.server.messages

import java.util.UUID

case class ReadSSTableRequest(blockUUId: UUID, subBlockUUId: UUID)