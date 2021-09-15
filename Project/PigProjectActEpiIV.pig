inputDialogues = LOAD 'hdfs:///user/root/input/episodeIV_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);

ranked = RANK inputDialogues;

OnlyDialogues= FILTER ranked BY (rank_inputDialogues > 2);

groupByName = GROUP OnlyDialogues By name;

names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;

namesOrdered = ORDER names BY no_of_lines desc;

STORE namesOrdered INTO 'hdfs:///user/root/output/episodeIV_output' USING PigStorage('\t');