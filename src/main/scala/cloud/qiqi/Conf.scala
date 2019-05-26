package cloud.qiqi

import java.time.LocalDate

import org.apache.commons.cli.{DefaultParser, HelpFormatter, Options, ParseException}

import scala.util.{Failure, Try}

case class Conf(startDate: LocalDate, daysToRun: Int, srcPath: String, dstPath: String)

object Conf {
  val options: Options = new Options()
    .addOption(null, "help", false, "print help")
    .addRequiredOption(null, "startDate", true, "Start date to process")
    .addRequiredOption(null, "daysToRun", true, "Number of days to run")
    .addRequiredOption(null, "srcPath", true, "Source directory path of raw files")
    .addRequiredOption(null, "dstPath", true, "Destination directory path of output results")

  def apply(args: Array[String]): Try[Conf] = Try {
    val cmd = new DefaultParser().parse(options, args)
    Conf(
      LocalDate.parse(cmd.getOptionValue("startDate")),
      cmd.getOptionValue("daysToRun").toInt,
      cmd.getOptionValue("srcPath"),
      cmd.getOptionValue("dstPath")
    )
  } recoverWith {
    case e: ParseException => new HelpFormatter().printHelp(e.getMessage, Conf.options)
      Failure(e)
    case e: Exception => e.printStackTrace()
      Failure(e)
  }
}