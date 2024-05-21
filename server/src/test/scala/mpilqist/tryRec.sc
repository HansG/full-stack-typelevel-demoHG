
object Rec :
  var nn =  () => println("next")
  
  def rec : Unit =
    def add(n : => Unit) : Unit = {
      nn()
      nn = () => n
    }
    
    add(rec)

import Rec.rec

rec
rec



