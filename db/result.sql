CREATE SCHEMA IF NOT EXISTS employee;
CREATE TABLE employee.employees IF NOT EXISTS (position VARCHAR(255), salary FLOAT, _id VARCHAR(255), age FLOAT, name VARCHAR(255));
INSERT INTO employee.employees (position, salary, _id, age, name) VALUES ('Engineer', 3767.925634753098, '64798c213f273a7ca2cf516c', 35, 'Raymond Monahan');
CREATE TABLE IF NOT EXISTS employee.employees_address (_id VARCHAR(255) PRIMARY KEY, employees__id VARCHAR(255), line1 VARCHAR(255), zip VARCHAR(255));
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('b93eeb53-8ff1-458f-87c5-7e0192b9cbf8', '32550 Port Gatewaytown', '18399', '64798c213f273a7ca2cf516c');
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('a9f6f61c-0f9f-4aef-9a6f-a1ebb1adaf81', '3840 Cornermouth', '83941', '64798c213f273a7ca2cf516c');
CREATE TABLE IF NOT EXISTS employee.employees_phone (_id VARCHAR(255) PRIMARY KEY, employees__id VARCHAR(255), personal VARCHAR(255), work VARCHAR(255));
INSERT INTO employee.employees_phone (_id, personal, work, student__id) VALUES ('7c2292bd-a358-48b9-a3dd-a6b311a47ccd', '8764255212', '2762135091', '64798c213f273a7ca2cf516c');
DELETE FROM student.students WHERE _id = '64798c213f273a7ca2cf516a';
CREATE SCHEMA IF NOT EXISTS student;
CREATE TABLE student.students IF NOT EXISTS (age FLOAT, name VARCHAR(255), subject VARCHAR(255), _id VARCHAR(255));
INSERT INTO student.students (age, name, subject, _id) VALUES (19, 'Tevin Heathcote', 'English', '64798c213f273a7ca2cf516d');
INSERT INTO employee.employees (age, name, position, salary, _id) VALUES (37, 'Wilson Gleason', 'Manager', 5042.121824095532, '64798c213f273a7ca2cf516e');
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('481888a5-f5dc-4750-b1fd-b7f5396c3ea3', '481 Harborsburgh', '89799', '64798c213f273a7ca2cf516e');
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('feb0c46a-38fa-4450-b67e-f515ffbd4169', '329 Flatside', '80872', '64798c213f273a7ca2cf516e');
INSERT INTO employee.employees_phone (_id, personal, work, student__id) VALUES ('efaa8ac5-0f99-4c9b-93bf-71a120315689', '7678456640', '8130097989', '64798c213f273a7ca2cf516e');
INSERT INTO employee.employees (age, name, position, salary, _id) VALUES (31, 'Linwood Wilkinson', 'Manager', 4514.763474407185, '64798c213f273a7ca2cf516f');
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('161bd345-6f83-4297-919a-5a5031fd2bad', '96400 Landhaven', '41638', '64798c213f273a7ca2cf516f');
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('06eb1909-d1ac-41f0-9034-cfc3c5b0230d', '3939 Lightburgh', '99747', '64798c213f273a7ca2cf516f');
INSERT INTO employee.employees_phone (_id, personal, work, student__id) VALUES ('fb6b8158-f2ff-4485-86cc-61ffbb30c0be', '1075027422', '1641587035', '64798c213f273a7ca2cf516f');
INSERT INTO student.students (_id, age, name, subject) VALUES ('64798c213f273a7ca2cf5170', 18, 'Camren Thompson', 'Science');
INSERT INTO employee.employees (age, name, position, salary, _id) VALUES (31, 'Meaghan Hettinger', 'Engineer', 6676.956103628756, '64798c213f273a7ca2cf5171');
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('384e530b-0e4b-403b-b022-ccf13d393081', '51338 Landingbury', '74795', '64798c213f273a7ca2cf5171');
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('5b69da6d-355f-4e66-b6be-96e8ad68a60c', '79033 West Locksmouth', '43555', '64798c213f273a7ca2cf5171');
INSERT INTO employee.employees_phone (_id, personal, work, student__id) VALUES ('7c21259c-80ef-4de6-ae3f-2a090b76cbbf', '4613562303', '1889316722', '64798c213f273a7ca2cf5171');
UPDATE employee.employees SET Age = 23 WHERE _id = '64798c213f273a7ca2cf5171';
ALTER TABLE employee.employees ADD workhours VARCHAR(255);
INSERT INTO employee.employees (workhours, _id, age, name, position, salary) VALUES (6, '64798c213f273a7ca2cf5172', 20, 'Delta Bahringer', 'Developer', 2980.1271103167737);
INSERT INTO employee.employees_phone (_id, personal, work, student__id) VALUES ('1f32ecf2-0829-4cb9-b789-f3d914b2155f', '9829848796', '5636590993', '64798c213f273a7ca2cf5172');
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('30c90049-5917-4d32-8f6e-f534af737587', '2787 Trackview', '23598', '64798c213f273a7ca2cf5172');
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('b7c079c7-8f5a-4ed4-bc74-965c97dc1305', '33659 South Mountainchester', '45086', '64798c213f273a7ca2cf5172');
ALTER TABLE student.students ADD is_graduated VARCHAR(255);
INSERT INTO student.students (age, is_graduated, name, subject, _id) VALUES (20, false, 'Freda Dare', 'Maths', '64798c213f273a7ca2cf5173');
INSERT INTO student.students (is_graduated, name, subject, _id, age) VALUES (true, 'Kamille Jast', 'Maths', '64798c213f273a7ca2cf5174', 23);
INSERT INTO student.students (subject, _id, age, is_graduated, name) VALUES ('Social Studies', '64798c213f273a7ca2cf5175', 19, false, 'Arden Kessler');
INSERT INTO employee.employees (salary, workhours, _id, age, name, position) VALUES (6322.655857670963, 4, '64798c213f273a7ca2cf5176', 29, 'Chyna Kihn', 'Salesman');
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('a5c761a4-b824-4382-8322-389103664c21', '403 Walksfurt', '75756', '64798c213f273a7ca2cf5176');
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('c719c182-317d-474e-adac-5b928c166329', '5012 Port Branchberg', '21969', '64798c213f273a7ca2cf5176');
INSERT INTO employee.employees_phone (_id, personal, work, student__id) VALUES ('9d1a2e15-e808-4ee0-b133-82b504b6d420', '1748534264', '2515301788', '64798c213f273a7ca2cf5176');
INSERT INTO employee.employees (_id, age, name, position, salary, workhours) VALUES ('64798c213f273a7ca2cf5177', 38, 'Madie Klein', 'Engineer', 9811.365188057007, 5);
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('8a8bdb8e-0e65-43ff-a17c-59910a17f6d2', '73628 Port Knollchester', '97436', '64798c213f273a7ca2cf5177');
INSERT INTO employee.employees_address (_id, line1, zip, student__id) VALUES ('a7410a8e-f8ec-4515-a5c7-a33d9ff4542d', '93072 Lake Skywayhaven', '87218', '64798c213f273a7ca2cf5177');
INSERT INTO employee.employees_phone (_id, personal, work, student__id) VALUES ('f856f9b8-7942-47fd-9a09-ecbe4b71e1d4', '1498807115', '9172896730', '64798c213f273a7ca2cf5177');
INSERT INTO student.students (name, subject, _id, age) VALUES ('Nathan Lindgren', 'Maths', '64798c213f273a7ca2cf516a', 25);
INSERT INTO student.students (subject, _id, age, name) VALUES ('English', '64798c213f273a7ca2cf516b', 18, 'Meggie Hoppe');
