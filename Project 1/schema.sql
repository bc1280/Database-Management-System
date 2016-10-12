/**
 * Project 1 - ER Modeling & Schema Design
 * Author: Bill Chang
 **/

CREATE TABLE Book (isbn CHAR (10) NOT NULL,
	title CHAR (80) NOT NULL,
	edition INT,
	genre CHAR (80),
	pages INT,
	avg_rating DOUBLE,
	PRIMARY KEY(isbn));
    
INSERT INTO Book (isbn, title, edition, genre, pages, avg_rating) VALUES (0061928127, "Beautiful Ruins", 1, "Fiction", 337, 3.68);
INSERT INTO Book (isbn, title, edition, genre, pages, avg_rating) VALUES (0439554934, "Harry Potter and the Sorcerer's Stone", 1, "Fantasy", 320, 4.42);

CREATE TABLE Publisher (publisherid CHAR (10) NOT NULL,
	publisher_name CHAR (80) NOT NULL,
	website CHAR (80),
	PRIMARY KEY(publisherid));
    
INSERT INTO Publisher (publisherid, publisher_name, website) VALUES ("0000000001", "Harper Collins", "https://www.harpercollins.com");
INSERT INTO Publisher (publisherid, publisher_name, website) VALUES ("0000000002", "Scholastic Inc.", "http://www.scholastic.com/home/");

CREATE TABLE Award (awardid CHAR (10) NOT NULL,
	award_name CHAR (80) NOT NULL,
	award_year INT NOT NULL,
	PRIMARY KEY(awardid));
    
INSERT INTO Award (awardid, award_name, award_year) VALUES ("0000000159", "Mythopoeic Fantasy Award", 2008);
INSERT INTO Award (awardid, award_name, award_year) VALUES ("0000000186", "British Book Award", 1998);
INSERT INTO Award (awardid, award_name, award_year) VALUES ("0000000780", "Smarties Prize", 1997);

CREATE TABLE Review (reviewid CHAR (10) NOT NULL,
	contents CHAR (500),
	rating INT NOT NULL,
	PRIMARY KEY(reviewid));

INSERT INTO Review (reviewid, contents, rating) VALUES ("0000000001", "This is the type of book that determines your next vacation.", 3);
INSERT INTO Review (reviewid, contents, rating) VALUES ("0000000002", "There are no words to do this book justice.", 5);

CREATE TABLE User (uid CHAR (10) NOT NULL,
	uname CHAR (80) NOT NULL,
	date_of_birth CHAR (10),
	nationality CHAR (20),
	PRIMARY KEY(uid));
    
INSERT INTO User (uid, uname, date_of_birth, nationality) VALUES ("0004527753", "Khanh", "03/03/1970", "United States");
INSERT INTO User (uid, uname, date_of_birth, nationality) VALUES ("0001077326", "J.K. Rowling", "31/07/1965", "United Kingdom");
INSERT INTO User (uid, uname, date_of_birth, nationality) VALUES ("0000012667", "Jess Walter", "01/01/1965", "United States");

CREATE TABLE Author (uid CHAR (10) NOT NULL,
	genre1 CHAR (80),
	genre2 CHAR (80),
	genre3 CHAR (80),
	PRIMARY KEY(uid),
	FOREIGN KEY(uid) REFERENCES User);

INSERT INTO Author (uid, genre1, genre2, genre3) VALUES ("0001077326", "Fiction", "Young Adult", "Fantasy");
INSERT INTO Author (uid, genre1, genre2, genre3) VALUES ("0000012667", "Literature & Fiction", "Mystery & Thrillers", "Nonfiction");

CREATE TABLE Reader (uid CHAR (10) NOT NULL,
	joined_year INT,
	PRIMARY KEY(uid),
	FOREIGN KEY(uid) REFERENCES User);

INSERT INTO Reader (uid, joined_year) VALUES ("0004527753", 2010);

CREATE TABLE Follow (ruid CHAR (10) NOT NULL,
	auid CHAR (10) NOT NULL,
	PRIMARY KEY(ruid, auid),
	FOREIGN KEY(ruid) REFERENCES Reader(uid),
	FOREIGN KEY(auid) REFERENCES Author(uid));

INSERT INTO Follow (ruid, auid) VALUES ("0004527753", "0001077326");

CREATE TABLE Criticize (uid CHAR (10) NOT NULL,
	reviewid CHAR (10) NOT NULL,
	PRIMARY KEY(uid, reviewid),
	FOREIGN KEY(uid) REFERENCES Reader ON DELETE CASCADE,
	FOREIGN KEY(reviewid) REFERENCES Review ON DELETE CASCADE);

INSERT INTO Criticize (uid, reviewid) VALUES ("0004527753", "0000000001");
INSERT INTO Criticize (uid, reviewid) VALUES ("0004527753", "0000000002");

CREATE TABLE Rate (isbn CHAR (10) NOT NULL,
	reviewid CHAR (10) NOT NULL,
	PRIMARY KEY(isbn, reviewid),
	FOREIGN KEY(isbn) REFERENCES Book ON DELETE CASCADE,
	FOREIGN KEY(reviewid) REFERENCES Review ON DELETE CASCADE);

INSERT INTO Rate (isbn, reviewid) VALUES ("0061928127", "0000000001");
INSERT INTO Rate (isbn, reviewid) VALUES ("0439554934", "0000000002");

CREATE TABLE ReadBy (uid CHAR (10) NOT NULL,
	isbn CHAR (10) NOT NULL,
	PRIMARY KEY(uid, isbn),
	FOREIGN KEY(uid) REFERENCES Reade,
	FOREIGN KEY(isbn) REFERENCES Book);

INSERT INTO ReadBy (uid, isbn) VALUES ("0004527753", "0061928127");
INSERT INTO ReadBy (uid, isbn) VALUES ("0004527753", "0439554934");

CREATE TABLE Writen (uid CHAR (10) NOT NULL,
	isbn CHAR (10) NOT NULL,
	PRIMARY KEY(uid, isbn),
	FOREIGN KEY(uid) REFERENCES Author,
	FOREIGN KEY(isbn) REFERENCES Book);

INSERT INTO Writen (uid, isbn) VALUES ("0000012667", "0061928127");
INSERT INTO Writen (uid, isbn) VALUES ("0001077326", "0439554934");

CREATE TABLE Publish (publisherid CHAR (10) NOT NULL,
	isbn CHAR (10) NOT NULL,
	publish_year INT NOT NULL,
	PRIMARY KEY(publisherid, isbn, publish_year),
	FOREIGN KEY(publisherid) REFERENCES Publisher,
	FOREIGN KEY(isbn) REFERENCES Book);

INSERT INTO Publish (publisherid, isbn, publish_year) VALUES ("0000000001", "0061928127", 2012);
INSERT INTO Publish (publisherid, isbn, publish_year) VALUES ("0000000002", "0439554934", 1997);

CREATE TABLE Win (awardid CHAR (10) NOT NULL,
	isbn CHAR (10) NOT NULL,
	PRIMARY KEY(awardid, isbn),
	FOREIGN KEY(awardid) REFERENCES Award,
	FOREIGN KEY(isbn) REFERENCES Book);

INSERT INTO Win (awardid, isbn) VALUES ("0000000159", "0439554934");
INSERT INTO Win (awardid, isbn) VALUES ("0000000186", "0439554934");
INSERT INTO Win (awardid, isbn) VALUES ("0000000780", "0439554934");


