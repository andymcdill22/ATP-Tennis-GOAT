#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  2 17:00:56 2023

@author: andrewmcdill
"""

import pandas as pd
import zipfile
import requests
from io import BytesIO


pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


#Data loading
links = ['http://tennis-data.co.uk/2000/2000.xls', 'http://tennis-data.co.uk/2001/2001.xls', 'http://tennis-data.co.uk/2002/2002.xls', 'http://tennis-data.co.uk/2003/2003.xls', 
         'http://tennis-data.co.uk/2004/2004.xls', 'http://tennis-data.co.uk/2005/2005.xls', 'http://tennis-data.co.uk/2006/2006.xls', 'http://tennis-data.co.uk/2007/2007.xls', 
         'http://tennis-data.co.uk/2008/2008.zip', 'http://tennis-data.co.uk/2009/2009.xls', 'http://tennis-data.co.uk/2010/2010.xls', 'http://tennis-data.co.uk/2011/2011.xls', 
         'http://tennis-data.co.uk/2012/2012.xls', 'http://tennis-data.co.uk/2013/2013.xlsx', 'http://tennis-data.co.uk/2014/2014.xlsx', 'http://tennis-data.co.uk/2015/2015.xlsx', 
         'http://tennis-data.co.uk/2016/2016.xlsx', 'http://tennis-data.co.uk/2017/2017.xlsx', 'http://tennis-data.co.uk/2018/2018.xlsx', 'http://tennis-data.co.uk/2019/2019.xlsx', 
         'http://tennis-data.co.uk/2020/2020.xlsx', 'http://tennis-data.co.uk/2021/2021.xlsx', 'http://tennis-data.co.uk/2022/2022.xlsx', 'http://tennis-data.co.uk/2023/2023.xlsx']


#Merge files into one dataframe
df = pd.DataFrame()
for i, elem in enumerate(links):
    if elem[-4:] == '.zip':
        content = requests.get(elem)
        zf = zipfile.ZipFile(BytesIO(content.content))
        temp = pd.read_excel(zf.open(zf.namelist()[0])) 
    else:
        temp = pd.read_excel(elem) 
    df = pd.concat([df, temp], ignore_index=True)
    

#Filter data
df = df[df.Winner.isin(['Federer R.', 'Nadal R.', 'Djokovic N.']) | df.Loser.isin(['Federer R.', 'Nadal R.', 'Djokovic N.'])].reset_index(drop=True)


#Data Preprocessing
df = df[~df['WRank'].isnull()].reset_index(drop=True)
df = df[~df['LRank'].isnull()].reset_index(drop=True)
df = df[~df['W1'].isnull()].reset_index(drop=True)
df = df[~df['W2'].isnull()].reset_index(drop=True)
df = df[~df['L1'].isnull()].reset_index(drop=True)
df = df[~df['L2'].isnull()].reset_index(drop=True)
df[['W3', 'W4', 'W5', 'L3', 'L4', 'L5']] = df[['W3', 'W4', 'W5', 'L3', 'L4', 'L5']].fillna(0)

df['B365W'] = df['B365W'].fillna(df[['CBW', 'GBW', 'IWW', 'SBW', 'B&WW', 'EXW', 'PSW', 'UBW', 'LBW', 'SJW']].mean(axis = 1)).fillna(df['AvgW'])
df['B365L'] = df['B365L'].fillna(df[['CBL', 'GBL', 'IWL', 'SBL', 'B&WL', 'EXL', 'PSL', 'UBL', 'LBL', 'SJL']].mean(axis = 1)).fillna(df['AvgL'])

df['ind'] = [(lambda x: x % 2)(x) for x in range(len(df))]

def checkempty(str):
    if str == ' ':
        return 0
    return str

df['W2'] = df['W2'].apply(checkempty)
df['L2'] = df['L2'].apply(checkempty)
df['W3'] = df['W3'].apply(checkempty)
df['L3'] = df['L3'].apply(checkempty)
df[['W1', 'L1', 'W2', 'L2', 'W3', 'L3', 'W4', 'L4', 'W5', 'L5']] = df[['W1', 'L1', 'W2', 'L2', 'W3', 'L3', 'W4', 'L4', 'W5', 'L5']].astype(float).astype(int)

df['Player_1'] = df.apply(lambda row: row['Winner'] if row['ind'] == 0 else row['Loser'], axis = 1)
df['Player_2'] = df.apply(lambda row: row['Winner'] if row['ind'] == 1 else row['Loser'], axis = 1)
df['Rank_1'] = df.apply(lambda row: row['WRank'] if row['ind'] == 0 else row['LRank'], axis = 1)
df['Rank_2'] = df.apply(lambda row: row['WRank'] if row['ind'] == 1 else row['LRank'], axis = 1)
df['Pts_1'] = df.apply(lambda row: row['WPts'] if row['ind'] == 0 else row['LPts'], axis = 1)
df['Pts_2'] = df.apply(lambda row: row['WPts'] if row['ind'] == 1 else row['LPts'], axis = 1)
df['Odd_1'] = df.apply(lambda row: row['B365W'] if row['ind'] == 0 else row['B365L'], axis = 1)
df['Odd_2'] = df.apply(lambda row: row['B365W'] if row['ind'] == 1 else row['B365L'], axis = 1)

def score(df):
    if df['ind'] == 0:
        return str(int(df['W1'])) + '-' + str(int(df['L1'])) + ' ' + str(int(df['W2'])) + '-' + str(int(df['L2'])) + ' ' + str(int(df['W3'])) + '-' + str(int(df['L3'])) +\
 ' ' + str(int(df['W4'])) + '-' + str(int(df['L4'])) + ' ' + str(int(df['W5'])) + '-' + str(int(df['L5'])) + ' '
    return str(int(df['L1'])) + '-' + str(int(df['W1'])) + ' ' + str(int(df['L2'])) + '-' + str(int(df['W2'])) + ' ' + str(int(df['L3'])) + '-' + str(int(df['W3'])) +\
 ' ' + str(int(df['L4'])) + '-' + str(int(df['W4'])) + ' ' + str(int(df['L5'])) + '-' + str(int(df['W5'])) + ' '
 
df['Score'] = df.apply(lambda row: score(row).replace('0-0', ''), axis = 1)

new_df = df[['Tournament', 'Date', 'Series', 'Court', 'Surface', 'Round', 'Best of', 'Player_1', 'Player_2','Winner', 'Rank_1', 'Rank_2', 'Pts_1', 'Pts_2', 'Odd_1', 'Odd_2', 'Score']]

def check(str):
    return str.replace('NR', -1)

new_df['Date'] = pd.to_datetime(new_df['Date'], format = '%Y-%M-%d')
new_df = new_df.fillna(-1)
new_df[['Best of', 'Rank_1', 'Rank_2', 'Pts_1', 'Pts_2']] = new_df[['Best of', 'Rank_1', 'Rank_2', 'Pts_1', 'Pts_2']].apply(check)
new_df[['Best of', 'Rank_1', 'Rank_2', 'Pts_1', 'Pts_2']] = new_df[['Best of', 'Rank_1', 'Rank_2', 'Pts_1', 'Pts_2']].astype(float).astype(int)

new_df.to_csv(r'/Users/andrewmcdill/Documents/Data Science/Tennis/Data/tennis_goats.csv', index=False)
