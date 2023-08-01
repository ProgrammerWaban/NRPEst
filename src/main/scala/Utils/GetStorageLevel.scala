package Utils

import org.apache.spark.storage.StorageLevel

object GetStorageLevel {
  def getStorageLevel(level: Int): StorageLevel={
    level match {
      case 1 => StorageLevel.DISK_ONLY
      case 2 => StorageLevel.MEMORY_AND_DISK
      case 3 => StorageLevel.MEMORY_ONLY_SER
      case _ => StorageLevel.MEMORY_ONLY
    }
  }
}
