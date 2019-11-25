package ua.ucu.edu

import scala.io.Source
import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class Employee(
  id: Int,
  firstName: String,
  lastName: String,
  email: String,
  hireDate: String,
  jobId: String,
  salary: Int,
  managerId: Int,
  departmentId: Int)

case class Department(
  departmentId: Int,
  departmentName: String,
  managerId: Int,
  locationId: Int)

object Traverse extends App {

//  for {
//    line <- Source.fromResource("employees.csv").getLines().toList
//        fields = line.split(",")
//      } println(fields(0) + "--" + fields(1) + "--" + fields(2) + "--" + fields(10))



  def parseEmployees: List[Employee] = {
    val formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy")
    for {
      line <- Source.fromResource("employees.csv").getLines().toList.tail
      fields = line.split(",")
  } yield {
    if (fields(9) == "NULL") //fields(5).trim().replace(fields(5).trim().substring(3,6), fields(5).trim().substring(3,6).toLowerCase.capitalize)
      Employee(fields(0).trim().toInt, fields(1), fields(2), fields(3), fields(5), fields(6), fields(7).trim().toInt, 0, fields(10).trim().toInt)
    else
      Employee(fields(0).trim().toInt, fields(1), fields(2), fields(3), fields(5), fields(6), fields(7).trim().toInt, fields(9).trim().toInt, fields(10).trim().toInt)
  }
  }

  //  val list: List[Employee] = parseEmployees
//  println(list)

  def parseDepartments: List[Department] = for {
    line <- Source.fromResource("departments.csv").getLines().toList.tail
    fields = line.split(",")
  } yield {
    Department(fields(0).trim().toInt, fields(1), fields(2).trim().toInt, fields(3).trim().toInt)
  }

//  val list: List[Department] = parseDepartments
//  println(list)

  def countEmpPerManager(manager: Employee, employeeList: List[Employee]): Int = {
    val groupedList: List[Employee] = employeeList.groupBy(_.managerId == manager.id).getOrElse(true, Nil)
    if (groupedList != Nil)
      groupedList.length
    else
      0
  }

    def getManagersAndEmpNum(employeeList: List[Employee]): List[(Employee, Int)] = {
      for {
        manager <- employeeList
        if countEmpPerManager(manager, employeeList) > 0
      } yield (manager, countEmpPerManager(manager, employeeList))
    }

  def getSalPerDep(employeeList: List[Employee], department: Department): List[Int] = {
      for {
        employee <- employeeList
        if employee.departmentId == department.departmentId
      } yield {
        employee.salary
      }
  }

  def getEmpWithLowSalPerDep(employeeList: List[Employee], departmentList: List[Department]): List[(Department, List[Employee])] = {
    for {
      department <- departmentList
      if employeeList.groupBy(_.departmentId == department.departmentId).getOrElse(true, Nil).nonEmpty
    } yield {
      (department, employeeList.groupBy(_.departmentId == department.departmentId).getOrElse(true, Nil).groupBy(_.salary == getSalPerDep(employeeList, department).min).getOrElse(true, Nil))
    }
  }

  def getEmpPerDepIfSumGT(employeeList: List[Employee], departmentList: List[Department], threshold: Int): List[(Department, List[Employee])] = {
    for {
      department <- departmentList
      if getSalPerDep(employeeList, department).sum > threshold;
      if employeeList.groupBy(_.departmentId == department.departmentId).getOrElse(true, Nil).nonEmpty
    } yield {
      (department, employeeList.groupBy(_.departmentId == department.departmentId).getOrElse(true, Nil))
    }
  }

  def getEmpHiredBe4Man(employeeList: List[Employee], departmentList: List[Department]): List[(Department, List[Employee])] = {
    for {
      department <- departmentList
    } yield {
      (department, employeeList.groupBy(_.departmentId == department.departmentId).getOrElse(true, Nil))
    }
  }


  val employees = parseEmployees
  val departments = parseDepartments
//  getManagersAndEmpNum(employees).foreach(println)

//  departments.foreach(println)
//  val dep = Department(90,"Administration",200,1700)
//  getEmpPerDep(employees, dep).foreach(println)
//  getEmpWithLowSalPerDep(employees, departments).foreach(println)
//getEmpPerDepIfSumGT(employees, departments, 34000).foreach(println)

//    employees.groupBy(_.hireDate.after().foreach(println)
//  getEmpHiredBe4Man(employees, departments)

//  val date = Date.parse("01-JAN-1990").
//  println(date)

//  val formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy")
//  val date = LocalDate.parse("16-Jun-1987", formatter)
//  println(date)
//
//  val str = "16-JUN-1987"
//  println(str.replace(str.substring(3,6), str.substring(3,6).toLowerCase.capitalize) )
}



