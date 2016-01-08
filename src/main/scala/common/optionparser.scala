package common

class OptionParser(builder: OptionParserBuilder) {
  var options: List[String] = builder.options
  var otypes: Map[Symbol, String] = builder.otypes

  type OptionMap = Map[Symbol, Any]
  def nextOption(list: List[String]): OptionMap = {
    nextOption(Map[Symbol, Any](), list)
  }
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    var ret = Map[Symbol, Any]()
    for (option <- options) {
      list match {
        case Nil => map
        case option :: value :: tail =>
          var curOpt = option
          while (curOpt.startsWith("-")) {
            curOpt = curOpt.substring(1)
          }
          val symbol = Symbol(curOpt)
          if (otypes(symbol) == "list") {
            var newMap = if (map.contains(symbol)) {
              val list = value :: map(symbol).asInstanceOf[List[String]]
              map ++ Map(symbol -> list)
            } else {
              val list = value :: Nil
              map ++ Map(symbol -> list)
            }
            ret = nextOption(newMap, tail)
          } else {
            ret = nextOption(map ++ Map(symbol -> value), tail)
          }
          return ret
        case option :: tail =>
          println("Unknown option " + option)
          System.exit(1)
          return map
      }
    }
    map
  }
}

class OptionParserBuilder {
  var options: List[String] = null
  var otypes: Map[Symbol, String] = null

  def withOptions(options: List[String]): OptionParserBuilder = {
    this.options = options
    this
  }
  def withOTypes(otypes: Map[Symbol, String]): OptionParserBuilder = {
    this.otypes = otypes
    this
  }

  def buildInstance(): OptionParser = {
    new OptionParser(this)
  }
}

object OptionParserBuilder {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParserBuilder()
      .withOptions(List("--input", "--output"))
      .withOTypes(Map('input -> "list", 'output -> "s")).buildInstance()
    for ((k, v) <- parser.nextOption(List("--input", "path1", "--input", "path2", "--output", "opath"))) {
      System.out.println(k + "," + v)
    }
    System.out.println(List("sdf", "sdb"))
  }
}

