--DB on master and Slaves
create database UserReg;
use UserReg;
CREATE TABLE IF NOT EXISTS Users (
  userName varchar(50) NOT NULL,
  userEmail varchar(250) NOT NULL UNIQUE,
  userPassword varchar(50) NOT NULL,
  PRIMARY KEY (userName)
);


-- DB on datanodes SQL
CREATE DATABASE data_nodes;
use data_nodes;
CREATE TABLE `node_table` (
    `node_number` INT,
 `is_node_alive` BOOLEAN, 
 `last_modified` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);
INSERT INTO node_table (node_number) VALUES (0),(1),(2);

CREATE TABLE `file_table` (
    `user_id` INT NOT NULL,
 `file_name` VARCHAR(255) NOT NULL,
  `node_number` INT, 
  `file_path` VARCHAR(255),
   `is_node_alive` BOOLEAN, 
   `last_modified` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);