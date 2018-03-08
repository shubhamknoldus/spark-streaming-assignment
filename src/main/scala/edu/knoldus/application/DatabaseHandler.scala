package edu.knoldus.application

import java.sql.{Connection, DriverManager, PreparedStatement}

object DatabaseHandler {
  Class.forName("com.mysql.jdbc.Driver")
  val CON: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/twitter", "root", "toor")
  val QUERY: String = "INSERT INTO Top_Hashtag VALUES(?, ?)"
  val PREPAREDSTATEMENT: PreparedStatement = CON.prepareStatement(QUERY)

  def insertInDataBase(hashTag: String, count: Int): Int = {
    PREPAREDSTATEMENT.setString(1, hashTag)
    PREPAREDSTATEMENT.setInt(2, count)
    PREPAREDSTATEMENT.executeUpdate()
  }
}
