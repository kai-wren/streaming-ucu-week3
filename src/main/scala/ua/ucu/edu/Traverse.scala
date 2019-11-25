package ua.ucu.edu

import scala.io.Source
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.parallel.immutable.ParSeq
import org.scalameter._

case class Employee(
  id: Int,
  firstName: String,
  lastName: String,
  email: String,
  hireDate: LocalDate,
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
//    fields = line.split(",")
//          } println(fields(0) + "--" + fields(1) + "--" + fields(2) + "--" + fields(10))

  //**********
  //Method to parse employees from csv file
  def parseEmployees: List[Employee] = {
    val formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy")
    for {
      line <- Source.fromResource("employees.csv").getLines().toList.tail
      fields = line.split(",")
  } yield {
    if (fields(9) == "NULL")
      Employee(fields(0).trim().toInt, fields(1), fields(2), fields(3),
        LocalDate.parse(fields(5).trim().replace(fields(5).trim().substring(3,6),
          fields(5).trim().substring(3,6).toLowerCase.capitalize), formatter), fields(6), fields(7).trim().toInt, 0,
        fields(10).trim().toInt)
    else
      Employee(fields(0).trim().toInt, fields(1), fields(2), fields(3),
        LocalDate.parse(fields(5).trim().replace(fields(5).trim().substring(3,6),
          fields(5).trim().substring(3,6).toLowerCase.capitalize), formatter), fields(6), fields(7).trim().toInt,
        fields(9).trim().toInt, fields(10).trim().toInt)
  }
  }

  //**********
  //Method to parse departments from csv file
  def parseDepartments: List[Department] = for {
    line <- Source.fromResource("departments.csv").getLines().toList.tail
    fields = line.split(",")
  } yield {
    Department(fields(0).trim().toInt, fields(1), fields(2).trim().toInt, fields(3).trim().toInt)
  }

    //**********
    //Method to return list of managers and number of employees for each of them using for/groupBy/reduce
    def getManagersAndEmpNum1(employeeList: List[Employee]): List[(Employee, Int)] = {
      for {
        manager <- employeeList
        if employeeList.groupBy(_.managerId == manager.id).getOrElse(true, Nil).nonEmpty
      } yield (manager, employeeList.groupBy(_.managerId == manager.id).getOrElse(true, Nil).length)
    }

  //**********
  //Method to return list of managers and number of employees for each of them using for/groupBy/reduce using parallel
  // collections
  def getManagersAndEmpNum1Par(employeeList: ParSeq[Employee]): ParSeq[(Employee, Int)] = {
    for {
      manager <- employeeList
      if employeeList.groupBy(_.managerId == manager.id).getOrElse(true, Nil).nonEmpty
    } yield (manager, employeeList.groupBy(_.managerId == manager.id).getOrElse(true, Nil).length)
  }

  //**********
  //Method to return list of managers and number of employees for each of them using map/flatMap/filter
  def getManagersAndEmpNum2(employeeList: List[Employee]): List[(Employee, Int)] = {
    employeeList.map(x => (x, employeeList.filter(_.managerId == x.id).length)).filter(_._2 > 0)
  }

  //**********
  //Method to return list of managers and number of employees for each of them using map/flatMap/filter using parallel
  // collections
  def getManagersAndEmpNum2Par(employeeList: ParSeq[Employee]): ParSeq[(Employee, Int)] = {
    employeeList.map(x => (x, employeeList.filter(_.managerId == x.id).length)).filter(_._2 > 0)
  }

  //**********
  //Method to return list of salaries for department. I have to create this method, otherwise I couln'd use reduce later
  // to find min or total value
  def getSalPerDep(employeeList: List[Employee], department: Department): List[Int] = {
      for {
        employee <- employeeList
        if employee.departmentId == department.departmentId
      } yield {
        employee.salary
      }
  }

  //**********
  //Method to return list of employees with lowest salary per department using for/groupBy/reduce
  def getEmpWithLowSalPerDep1(employeeList: List[Employee], departmentList: List[Department]):
  List[(Department, List[Employee])] = {
    for {
      department <- departmentList
      if employeeList.groupBy(_.departmentId == department.departmentId).getOrElse(true, Nil).nonEmpty
    } yield {
      (department, employeeList.groupBy(_.departmentId == department.departmentId).getOrElse(true, Nil)
        .groupBy(_.salary == getSalPerDep(employeeList, department).reduce(_ min _)).getOrElse(true, Nil))
    }
  }

  //**********
  //Method to return list of employees with lowest salary per department using map/flatMap/filter
  def getEmpWithLowSalPerDep2(employeeList: List[Employee], departmentList: List[Department]):
  List[(Department, List[Employee])] = {
    departmentList.map(x => (x, employeeList.filter(_.departmentId == x.departmentId),
      employeeList.filter(_.departmentId == x.departmentId).map(y => y.salary) )).filter(_._2.nonEmpty)
      .map(y => (y._1, y._2.filter(_.salary == y._3.min) ))
  }

  //**********
  //Method to return list of employees per department where total department salary greater than threshold using
  // for/groupBy/reduce
  def getEmpPerDepIfSumGT1(employeeList: List[Employee], departmentList: List[Department], threshold: Int):
  List[(Department, List[Employee])] = {
    for {
      department <- departmentList
      if employeeList.groupBy(_.departmentId == department.departmentId).getOrElse(true, Nil).nonEmpty;
      if getSalPerDep(employeeList, department).reduce(_+_) > threshold
    } yield {
      (department, employeeList.groupBy(_.departmentId == department.departmentId).getOrElse(true, Nil))
    }
  }

  //**********
  //Method to return list of employees per department where total department salary greater than threshold using
  // map/flatMap/filter
  def getEmpPerDepIfSumGT2(employeeList: List[Employee], departmentList: List[Department], threshold: Int):
  List[(Department, List[Employee])] = {
    departmentList.map(x => (x, employeeList.filter(_.departmentId == x.departmentId),
      employeeList.filter(_.departmentId == x.departmentId).map(y => y.salary) )).filter(_._2.nonEmpty)
      .filter(_._3.sum > threshold).map(y => (y._1, y._2))
  }

  //**********
  //Method to return list of employees per department who were hired earlier than their manager using for/groupBy/reduce
  //To make this possible I have add 3 new employees to the csv file
  def getEmpHiredBe4Man1(employeeList: List[Employee], departmentList: List[Department]):
  List[(Department, List[Employee])] = {
    for {
      department <- departmentList
      if employeeList.groupBy(_.departmentId == department.departmentId).getOrElse(true, Nil).nonEmpty
      if employeeList.groupBy(_.departmentId == department.departmentId).getOrElse(true, Nil)
        .groupBy(_.hireDate.isBefore(employeeList.groupBy(_.id == department.managerId).getOrElse(true, Nil).head.hireDate))
        .getOrElse(true, Nil).nonEmpty
    } yield {
      (department, employeeList.groupBy(_.departmentId == department.departmentId).getOrElse(true, Nil)
        .groupBy(_.hireDate.isBefore(employeeList.groupBy(_.id == department.managerId).getOrElse(true, Nil).head.hireDate))
        .getOrElse(true, Nil))
    }
  }

  //**********
  //Method to return list of employees per department who were hired earlier than their manager using map/flatMap/filter
  //To make this possible I have add 3 new employees to the csv file
  def getEmpHiredBe4Man2(employeeList: List[Employee], departmentList: List[Department]):
  List[(Department, List[Employee])] = {
    departmentList.map(x => (x, employeeList.filter(_.departmentId == x.departmentId) )).filter(_._2.nonEmpty)
      .map(x => (x._1, x._2.filter(_.hireDate.isBefore(x._2.filter(_.id == x._1.managerId).head.hireDate)) ))
      .filter(_._2.nonEmpty)
  }

  //parsing csv files to be used later
  val employees = parseEmployees
  val departments = parseDepartments

  //executing usual and parallel versions of firs method with measurements
  val time1seq = measure {
    getManagersAndEmpNum1(employees)
  }
  println("Method getManagersAndEmpNum1 non-parallel executed in "+time1seq)
  println("**********")
  val time1par = measure {
  getManagersAndEmpNum1Par(employees.par)
  }
  println("Method getManagersAndEmpNum1Par parallel executed in "+time1par)
  println("**********")
  val time2seq = measure {
  getManagersAndEmpNum2(employees)
  }
  println("Method getManagersAndEmpNum2 non-parallel executed in "+time2seq)
  println("**********")
  val time2par = measure {
  getManagersAndEmpNum2Par(employees.par)
  }
  println("Method getManagersAndEmpNum2Par parallel executed in "+time2par)

  //  getEmpWithLowSalPerDep1(employees, departments).foreach(println)
//  println("**********")
//  getEmpWithLowSalPerDep2(employees, departments).foreach(println)

//  getEmpPerDepIfSumGT1(employees, departments, 44000).foreach(println)
//  println("**********")
//  getEmpPerDepIfSumGT2(employees, departments, 44000).foreach(println)

//  getEmpHiredBe4Man1(employees, departments).foreach(println)
//  println("**********")
//  getEmpHiredBe4Man2(employees, departments).foreach(println)


}



