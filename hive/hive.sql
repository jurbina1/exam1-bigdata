# You sql follows

create external table schools (sregion STRING, sdistrict STRING, scity STRING, sId INT, sName STRING, slevel STRING, sserie INT) 
    row format delimited
    fields terminated by ',';

LOAD DATA LOCAL INPATH '/Users/jady.urbina/Desktop/exam1-sp17-bigdata-desc-master/hive/escuelasPR.csv' OVERWRITE INTO TABLE schools;


create external table students (sregion STRING, sdistrict STRING, sId INT, sName STRING, slevel STRING, stsex STRING, stId int) 
    row format delimited
    fields terminated by ',';

LOAD DATA LOCAL INPATH '/Users/jady.urbina/Desktop/exam1-sp17-bigdata-desc-master/hive/studentsPR.csv' OVERWRITE INTO TABLE students;


#1
select T1.sregion, T1.scity, count(*)
from schools as T1, students as T2
where T1.sregion = T2.sregion
group by T1.sregion, T1.scity;

#2
select T1.scity, T1.slevel, count(*)
from schools as T1
group by T1.scity, T1.slevel;

#3
select count(*)
from schools as T1, students as T2
where T1.sId = T2.sId and
T1.slevel == 'Superior' and 
T1.scity == 'Ponce' and 
T2.stsex == 'F';

#4
select T1.sregion, T1.sdistrict, T1.scity, count(*)
from schools as T1, students as T2
where T1.sid = T2.sid and T2.stsex == 'M'
group by T1.sregion, T1.sdistrict, T1.scity;
