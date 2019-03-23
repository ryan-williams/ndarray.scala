package org.lasersonlab.hdf5

import org.lasersonlab.files.Local
import org.lasersonlab.hdf5.Header.V0
import org.lasersonlab.test.future.Assert
import utest._

object HeaderTest
extends lasersonlab.Suite
  with Assert.syntax {
  override implicit val ec = lasersonlab.threads.pool.`1`
  val tests = Tests {
    'load - {
      val file = Local("/Users/ryan/c/hdf5-java-cloud/files/ica_bone_marrow_h5.h5")
      for {
        hdr ← Header(file)
        _ ← ==(
          hdr,
          V0(
            0, 509539929, SymbolTable.Entry.Object(0, 96, Some(ScratchPad(136, 680)))
          )
        )
      } yield
        ()
    }
  }
}
