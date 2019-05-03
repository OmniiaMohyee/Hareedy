--mysql -u root -p
--enter password

--run the following script for creation of DB and Users table



--multiple databases for the sake of testing!!!

create database db1;
use db1;
CREATE TABLE IF NOT EXISTS Users (
  userName varchar(50) NOT NULL,
  userEmail varchar(250) NOT NULL UNIQUE,
  userPassword varchar(50) NOT NULL,
  loggedIn varchar(1) NOT NULL,
  PRIMARY KEY (userName)
);

create database db2;
use db2
CREATE TABLE IF NOT EXISTS Users (
  userName varchar(50) NOT NULL,
  userEmail varchar(250) NOT NULL UNIQUE,
  userPassword varchar(50) NOT NULL,
  loggedIn varchar(1) NOT NULL,
  PRIMARY KEY (userName)
);

create database db0;
use db0;
CREATE TABLE IF NOT EXISTS Users (
  userName varchar(50) NOT NULL,
  userEmail varchar(250) NOT NULL UNIQUE,
  userPassword varchar(50) NOT NULL,
  loggedIn varchar(1) NOT NULL,
  PRIMARY KEY (userName)
);


drop database db0;
drop database db1;
drop database db2;
