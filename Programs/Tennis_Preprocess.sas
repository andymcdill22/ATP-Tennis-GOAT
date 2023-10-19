/*ATP Tennis data preprocessing*/ 
options symbolgen mlogic mprint; 

**Import filtered, minorly pre processed raw data to continue preprocessing; 
proc import out=tennis datafile="/SAS/development/users/mcdila01/Tennis/Data/tennis_goats.csv" dbms=csv replace; guessingrows=3000; 


data tennis;
	set tennis;
	**Assign tournament weight based on series; 
	if series in ('International', 'ATP250') then tourney_weight=250;
	else if series in ('International Gold', 'ATP500') then tourney_weight=500;
	else if series in ('Masters', 'Masters 1000') then tourney_weight=1000;
	else if series in ('Grand Slam', 'Masters Cup') then tourney_weight=1500; 

	if winner=player_1 then loser=player_2;
	else if winner=player_2 then loser=player_1; 

	**Calculate games won over games lost; 
	set1=scan(score,1,' '); 
	set2=scan(score,2,' '); 
	set3=scan(score,3,' ');
	set4=scan(score,4,' '); 
	set5=scan(score,5,' '); 
	
	if set3=' ' then set3='0-0';
	if set4=' ' then set4='0-0';
	if set5=' ' then set5='0-0'; 

	gameswon_1=input(scan(set1,1,'-'), best.) + input(scan(set2,1,'-'), best.) + input(scan(set3,1,'-'), best.) + input(scan(set4,1,'-'), best.) + input(scan(set5,1,'-'), best.);
	gameslost_1=input(scan(set1,-1,'-'), best.) + input(scan(set2,-1,'-'), best.) + input(scan(set3,-1,'-'), best.) + input(scan(set4,-1,'-'), best.) + input(scan(set5,-1,'-'), best.);
	gameswon_2=input(scan(set1,-1,'-'), best.) + input(scan(set2,-1,'-'), best.) + input(scan(set3,-1,'-'), best.) + input(scan(set4,-1,'-'), best.) + input(scan(set5,-1,'-'), best.);
	gameslost_2=input(scan(set1,1,'-'), best.) + input(scan(set2,1,'-'), best.) + input(scan(set3,1,'-'), best.) + input(scan(set4,1,'-'), best.) + input(scan(set5,1,'-'), best.);

	game_ratio_1=round(gameswon_1/gameslost_1,0.001); 
	game_ratio_2=round(gameswon_2/gameslost_2,0.001); 

	if game_ratio_1=. then game_ratio_1=gameswon_1;
	if game_ratio_2=. then game_ratio_2=gameswon_2; 

	drop set1 set2 set3 set4 set5; 
run; 

proc sort data=tennis;
	by date round court surface series tournament tourney_weight winner loser game_ratio_1 game_ratio_2 rank_1 rank_2 pts_1 pts_2 odd_1 odd_2;
run; 

**Transpose data; 
proc transpose data=tennis out=tennis_trans;
	by date round court surface series tournament tourney_weight winner loser game_ratio_1 game_ratio_2 rank_1 rank_2 pts_1 pts_2 odd_1 odd_2;
	var player_1 player_2;
run; 

data tennis_trans;
	set tennis_trans (rename=(_NAME_=player_num col1=player));
	**One variable for each demographic; 
	if player in ('Federer R.','Nadal R.','Djokovic N.'); 
	if player_num='Player_1' then do;
		game_ratio=game_ratio_1; rank=rank_1; points=pts_1; odds=odd_1; 
	end; 
	else if player_num='Player_2' then do;
		game_ratio=game_ratio_2; rank=rank_2; points=pts_2; odds=odd_2; 
	end; 

	**Retrieve year and week number from date of match; 
	year = year(date); 
	week = week(date, 'w'); 
run; 

**Fix two date errors;
**1. China Open 2013 has dates as 2012; 
proc sort data=tennis_trans;
	by year tournament player round; 
run; 

proc freq data=tennis_trans;
	tables year*tournament*player*round/out=tourneycount noprint;
run;

data tourneycount;
	set tourneycount;
	if count gt 1 and round ne 'Round Robin';
run; 

proc sort data=tourneycount;
	by year tournament player round;
run; 

data tennis_trans;
	merge tennis_trans tourneycount;
	by year tournament player round;
run; 
	
data tennis_trans;
	set tennis_trans;
	if count ne . and points=11120 then do;
		corr_date=mdy(month(date), day(date), 2013); 
	end; 
	else do;
		corr_date=mdy(month(date), day(date), year(date));
	end;
	**Sony Ericsson final is wrong month in 2017; 
	if player in ('Nadal R.','Federer R.') and year=2017 and tournament='Sony Ericsson Open' and month(date) lt 3 then do;
		corr_date=mdy(04, day(date), year(date)); 
	end; 
	format corr_date mmddyy10.;
	year = year(corr_date);
	week = week(corr_date, 'v'); 
	drop count percent;
run;

proc sort data=tennis_trans;
	by year week;
run; 

**Export preprocessed raw data for first set of visuals; 
proc export data=tennis_trans outfile="/SAS/development/users/mcdila01/Tennis/Data/Output/tennis_final1.csv" dbms=csv replace; run; 

**Use raw data from kaggle to merge number 1 ranked players to compare with GOAT rankings; 
proc import out=atp_tennis datafile="/SAS/development/users/mcdila01/Tennis/Data/atp_tennis.csv" dbms=csv replace; guessingrows=3000; 

data rank_oth;
	set atp_tennis;
	if rank_1 < rank_2 then do;
		rank=rank_1;
		player=player_1;
	end; 
	else if rank_1 > rank_2 then do;
		rank=rank_2;
		player=player_2;
	end; 
	if rank=1; 
	year=year(date);
	week=week(date, 'v'); 
	keep year week player rank;
run; 

proc sort data=rank_oth;
	by year week;
run; 

**Create dataset with all weeks from 2000 to 2023; 
%macro date_insert; 
proc sql;
	create table date_trellis (
		year int,
		week int); 
quit;

%do y=2000 %to 2023; 
	%do w=1 %to 52;
	proc sql;
	insert into date_trellis(year, week) 
	values(&y., &w.); 
	%end;
%end; 
%mend date_insert; 

%date_insert;

proc sort data=date_trellis;
	by year week;
run; 

%macro player_eval;
*Sort by 3 players to get correct rankings for each week; 
%let playlab1=fed; %let player1=%str(Federer R.); 
%let playlab2=nad; %let player2=%str(Nadal R.); 
%let playlab3=djok; %let player3=%str(Djokovic N.); 

%do p=1 %to 3; 
data &&playlab&p.;
	set tennis_trans;
	if player="&&player&p."; 
run; 

data &&playlab&p.;
	merge &&playlab&p. date_trellis (in=flgd);
	by year week;
run; 

data &&playlab&p.;
	merge &&playlab&p. rank_oth (rename=(player=player_oth rank=rank_oth)); 
	by year week;
run; 

proc sort data=&&playlab&p.;
	by player year week;
run;

data career_&&playlab&p.;
	set &&playlab&p.;
	by player year week;
	car_start=first.player;
	car_end=last.player; 
	if player ne ' '; 
run; 

data _null_;
	set career_&&playlab&p.;
	if car_start=1 then do;
		call symput("year_start", year); 
		call symput("week_start", week); 
	end;
	if car_end=1 then do;
		call symput("year_end", year); 
		call symput("week_end", week); 
	end; 
run; 

data &&playlab&p.;
	set &&playlab&p.;
	if (year eq &year_start. and week lt &week_start.) or (year lt &year_start.) then delete;
	if (year eq &year_end. and week gt &week_end.) or (year gt &year_end.) then delete; 
run;

%do l=1 %to 50; 
proc sort data=&&playlab&p.;
	by year week;
run; 

data &&playlab&p.;
	set &&playlab&p.;
	by year week;
	lrank=lag(rank); 
run;

data &&playlab&p.;
	set &&playlab&p.;
	if rank=. then rank=lrank;
	if player eq . then player="&&player&p.";
	
	if rank=1 then do;
		if player_oth ne ' ' and player_oth ne player then rank=.; 
	end;
run;
%end; 

proc sort data=&&playlab&p.;
	by year week player; 
run; 

%end; 
%mend player_eval; 

%player_eval; 

data tennis_all;
	set fed nad djok;
	by year week player;
	keep player rank points year week corr_date; 
run; 

proc sort data=tennis_all;
	by year week;
run; 

proc sort data=tennis_all nodupkey out=tennis_trellis;
	by player year week rank;
run; 

**Create absent variable to indicate if there was a gap over 12 weeks in their career; 
data point;
	set tennis_trellis;
	by player year week; 
	point=_n_+0; 
	if corr_date ne .; 
	lpoint=lag(point); 
	gap=(point-lpoint)-2; 
	if gap > 12 then absent='Y'; 
	drop lpoint gap; 
run; 

proc sort data=point;
	by player descending year descending week;
run;

data point;
	set point;
	by player descending year descending week;
	lpoint=lag(point);
	gap=(lpoint-point)-2; 
	if gap > 12 then absent='Y'; 
	drop lpoint gap; 
run; 

data point;
	set point;
	if absent='Y'; 
	keep player year week absent; 
run; 

proc sort data=point;
	by player year week;
run; 

data tennis_trellis;
	merge tennis_trellis point;
	by player year week;
	drop rank_oth player_oth; 
run;

proc sort data=tennis_trellis;
	by year week;
run;

**Export non duplicated date data with gaps in career; 
proc export data=tennis_trellis outfile="/SAS/development/users/mcdila01/Tennis/Data/Output/tennis_final2.csv" dbms=csv replace; run; 


