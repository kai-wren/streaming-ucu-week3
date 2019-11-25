package ua.edu.ucu

import org.scalatest._
import ua.ucu.edu.Traverse._
import java.time.LocalDate

class TraverseSpec extends FlatSpec with Matchers {

  "An employees" should "contains list of employees from file employees.csv" in {
    val employees = parseEmployees
    employees.length should be (13)
    employees(0).salary should be (24000)
    employees(1).id should be (101)
    employees(2).hireDate should be (LocalDate.of(1993, 1, 13))
    employees(3).departmentId should be (60)
    employees(4).managerId should be (103)
    employees(5).email should be ("DAUSTIN")
    employees(6).firstName should be ("Valli")
    employees(7).lastName should be ("Lorentz")
    employees(8).jobId should be ("FI_MGR")
  }

  "A departments" should "contains list of departments from file departments.csv" in {
    val departments = parseDepartments
    departments.length should be (5)
    departments(0).managerId should be (200)
    departments(1).departmentId should be (20)
    departments(2).departmentName should be ("Purchasing")
    departments(3).locationId should be (2400)
  }

  "A managers1" should "contains list of managers with number of subordinate employees" in {
    val managers1 = getManagersAndEmpNum1(parseEmployees)
    managers1.length should be (5)
    managers1(0)._1.id should be (100)
    managers1(4)._1.id should be (108)
    managers1(0)._2 should be (2)
    managers1(4)._2 should be (1)
  }
//for parallel methods checking only length since order of items may not be preserved
  "A managers2" should "contains list of managers with number of subordinate employees" in {
    val managers2 = getManagersAndEmpNum1Par(parseEmployees.par)
    managers2.length should be (5)
  }

  "A managers3" should "contains list of managers with number of subordinate employees" in {
    val managers3 = getManagersAndEmpNum2(parseEmployees)
    managers3.length should be (5)
    managers3(0)._1.id should be (100)
    managers3(4)._1.id should be (108)
    managers3(0)._2 should be (2)
    managers3(4)._2 should be (1)
  }

  //for parallel methods checking only length since order of items may not be preserved
  "A managers4" should "contains list of managers with number of subordinate employees" in {
    val managers4 = getManagersAndEmpNum2Par(parseEmployees.par)
    managers4.length should be (5)
  }

  "A employees1" should "contain list of employees per department with lowest salary" in {
    val employees1 = getEmpWithLowSalPerDep1(parseEmployees, parseDepartments)
    employees1.length should be (3)
    employees1(0)._1.departmentId should be (90)
    employees1(0)._2.head.id should be (101)
    employees1(2)._1.departmentId should be (100)
    employees1(2)._2.head.id should be (109)
  }

  //for parallel methods checking only length since order of items may not be preserved
  "A employees2" should "contain list of employees per department with lowest salary" in {
    val employees2 = getEmpWithLowSalPerDep1Par(parseEmployees.par, parseDepartments.par)
    employees2.length should be (3)
  }

  "A employees3" should "contain list of employees per department with lowest salary" in {
    val employees3 = getEmpWithLowSalPerDep2(parseEmployees, parseDepartments)
    employees3.length should be (3)
    employees3(0)._1.departmentId should be (90)
    employees3(0)._2.head.id should be (101)
    employees3(2)._1.departmentId should be (100)
    employees3(2)._2.head.id should be (109)
  }

  //for parallel methods checking only length since order of items may not be preserved
  "A employees4" should "contain list of employees per department with lowest salary" in {
    val employees4 = getEmpWithLowSalPerDep2Par(parseEmployees.par, parseDepartments.par)
    employees4.length should be (3)
  }

  "A employees5" should "contain list of employees per department if total department salary bigger than threshold" in {
    val employees5 = getEmpPerDepIfSumGT1(parseEmployees, parseDepartments, 44000)
    employees5.length should be (1)
    employees5(0)._1.departmentId should be (90)
    employees5(0)._2.head.id should be (100)
    employees5(0)._2.length should be (4)
  }

  //for parallel methods checking only length since order of items may not be preserved. Since here only 1 line we
  // could check some other properties
  "A employees6" should "contain list of employees per department if total department salary bigger than threshold" in {
    val employees6 = getEmpPerDepIfSumGT1Par(parseEmployees.par, parseDepartments.par, 44000)
    employees6.length should be (1)
    employees6(0)._1.departmentId should be (90)
    employees6(0)._2.length should be (4)
  }

  "A employees7" should "contain list of employees per department if total department salary bigger than threshold" in {
    val employees7 = getEmpPerDepIfSumGT2(parseEmployees, parseDepartments, 44000)
    employees7.length should be (1)
    employees7(0)._1.departmentId should be (90)
    employees7(0)._2.head.id should be (100)
    employees7(0)._2.length should be (4)
  }

  //for parallel methods checking only length since order of items may not be preserved. Since here only 1 line we
  // could check some other properties
  "A employees8" should "contain list of employees per department if total department salary bigger than threshold" in {
    val employees8 = getEmpPerDepIfSumGT2Par(parseEmployees.par, parseDepartments.par, 44000)
    employees8.length should be (1)
    employees8(0)._1.departmentId should be (90)
    employees8(0)._2.length should be (4)
  }

  "A employees9" should "contain list of employees per department which are hired before manager of department" in {
    val employees9 = getEmpHiredBe4Man1(parseEmployees, parseDepartments)
    employees9.length should be (2)
    employees9(0)._1.departmentId should be (90)
    employees9(0)._2.length should be (1)
    employees9(0)._2.head.id should be (100)
    employees9(1)._1.departmentId should be (60)
    employees9(1)._2.length should be (2)
    employees9(1)._2.head.id should be (103)
  }

  //for parallel methods checking only length since order of items may not be preserved.
  "A employees10" should "contain list of employees per department which are hired before manager of department" in {
    val employees10 = getEmpHiredBe4Man1Par(parseEmployees.par, parseDepartments.par)
    employees10.length should be (2)
  }

  "A employees11" should "contain list of employees per department which are hired before manager of department" in {
    val employees11 = getEmpHiredBe4Man2(parseEmployees, parseDepartments)
    employees11.length should be (2)
    employees11(0)._1.departmentId should be (90)
    employees11(0)._2.length should be (1)
    employees11(0)._2.head.id should be (100)
    employees11(1)._1.departmentId should be (60)
    employees11(1)._2.length should be (2)
    employees11(1)._2.head.id should be (103)
  }

  //for parallel methods checking only length since order of items may not be preserved.
  "A employees12" should "contain list of employees per department which are hired before manager of department" in {
    val employees12 = getEmpHiredBe4Man2Par(parseEmployees.par, parseDepartments.par)
    employees12.length should be (2)
  }

}
