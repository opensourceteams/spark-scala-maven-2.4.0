
drop table if exists `test`;
CREATE TABLE `test` (
                       `id` int(11) NOT NULL AUTO_INCREMENT,
                       `name` varchar(255) DEFAULT NULL,
                       PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


drop table if exists `test2`;
CREATE TABLE `test2` (
                       `id` int(11) NOT NULL AUTO_INCREMENT,
                       `name` varchar(255) DEFAULT NULL,
                       PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



select * from test;

select * from test2;

show tables;