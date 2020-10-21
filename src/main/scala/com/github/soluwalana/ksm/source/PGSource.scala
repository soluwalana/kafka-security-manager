package com.github.soluwalana.ksm.source

import java.io._
import java.util.Date
import java.sql.Timestamp

import scala.slick.driver.PostgresDriver.simple._
import com.typesafe.config.Config
import org.slf4j.LoggerFactory


class PGSourceAcl extends SourceAcl {

  private val log = LoggerFactory.getLogger(classOf[PGSourceAcl])

  /**
    * Config Prefix for configuring this module
    */
  override val CONFIG_PREFIX: String = "pg"

  final val HOST_CONFIG = "host"
  final val PORT_CONFIG = "port"
  final val CA_CERT_CONFIG = "ca"
  final val CLIENT_CERT_CONFIG = "cert"
  final val CLIENT_KEY_CONFIG = "key"
  final val USER_CONFIG = "user"
  final val PASSWORD_CONFIG = "password"
  final val DATABASE_CONFIG = "database"

  var lastModified: Timestamp = new Timestamp(0)
  var connectionURL: String = _

  // this is a class that represents the table I've created in the database
  class KafkaAccess(tag: Tag) extends Table[(String, String, String, String, String, String, String, String, Boolean, Timestamp, Timestamp)](tag, "kafka_acls") {
    def id = column[String]("id")
    def principal = column[String]("principal")
    def resourceType = column[String]("resource_type")
    def patternType = column[String]("pattern_type")
    def resourceName = column[String]("resource_name")
    def operation = column[String]("operation")
    def permissionType = column[String]("permission_type")
    def host = column[String]("host")
    def deleted = column[Boolean]("deleted")
    def modifiedAt = column[Timestamp]("modified_at")
    def createdAt = column[Timestamp]("created_at")
    def * = (id, principal, resourceType, patternType, resourceName, operation, permissionType, host, deleted, modifiedAt, createdAt)
  }

  /**
    * internal config definition for the module
    */
  override def configure(config: Config): Unit = {
    val host = config.getString(HOST_CONFIG)
    val port = config.getString(PORT_CONFIG)
    val caCert = config.getString(CA_CERT_CONFIG)
    val clientCert = config.getString(CLIENT_CERT_CONFIG)
    val clientKey = config.getString(CLIENT_KEY_CONFIG)
    val user = config.getString(USER_CONFIG)
    val password = config.getString(PASSWORD_CONFIG)
    val database = config.getString(DATABASE_CONFIG)

    if (password != "") {
      connectionURL = s"jdbc:postgresql://$host:$port/$database?user=$user&password=$password"
    } else {
      connectionURL = s"jdbc:postgresql://$host:$port/$database?user=$user&sslrootcert=$caCert&sslcert=$clientCert&sslkey=$clientKey&sslmode=verify-full"
    }
    log.warn(s"Connection url being used $connectionURL")
  }

  /**
    * Refresh the current view on the external source of truth for Acl
    * Ideally this function is smart and does not pull the entire external Acl at every iteration
    * Return `None` if the Source Acls have not changed (usually using metadata).
    * Return `Some(x)` if the Acls have changed. `x` represents the parsing and parsing errors if any
    * Note: the first call to this function should never return `None`.
    *
    * Kafka Security Manager will not update Acls in Kafka until there are no errors in the result
    *
    * @return
    */
  override def refresh(): Option[Reader] = {
    var csv: String = ""
    Database.forURL(connectionURL, driver = "org.postgresql.Driver") withSession {
      implicit session => {
        val acl = TableQuery[KafkaAccess]
        val x = acl.map(_.modifiedAt).max.run
        var latest = lastModified
        if (!x.isEmpty) {
          latest = x.get
        }
        if (latest.after(lastModified)) {
          lastModified = latest
          csv = acl.filter(
            _.deleted === false
          ).map(
            x => (
              x.principal,
              x.resourceType,
              x.patternType,
              x.resourceName,
              x.operation,
              x.permissionType,
              x.host
            )
          ).list.map(
            _.productIterator.mkString(",")
          ).mkString("\n")
          log.warn(s"The csv for the acls was $csv in function")
        } else {
          log.warn(s"LATEST NOT EXPECTED ${latest.toString} ${lastModified.toString}")
        }
      }
    }
    log.warn(s"The csv for the acls was $csv out of function")
    if (csv == "") {
      return None
    }
    return Some(new BufferedReader(new StringReader(csv)))
  }

  /**
    * Close all the necessary underlying objects or connections belonging to this instance
    */
  override def close(): Unit = {
    // PG
  }
}
