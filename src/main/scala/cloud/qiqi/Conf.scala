package cloud.qiqi

import java.time.LocalDate

import picocli.CommandLine.Option

object Conf {
  @Option(names = Array("--startDate"), required = true, description = Array("start date to process"))
  var startDate: LocalDate = _

  @Option(names = Array("--daysToRun"), required = true, description = Array("number of days to run"))
  var daysToRun: Int = _

  @Option(names = Array("--srcPath"), required = true, description = Array("source parent directory path: raw csv files"))
  var srcPath: String = _

  @Option(names = Array("--dstPath"), required = true, description = Array("destination parent directory path: output results"))
  var dstPath: String = _
}