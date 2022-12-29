Q1. How do you load a CSV file into a Pandas DataFrame?

ans - To load a csv file into a pandas dataframe we can use the read_csv method of the pandas library which returns a 2D dataframe as a result.

```
import pandas as pd
df=pd.read_csv('winequality-red.csv')   # put the path of the csv file
```

Q2. How do you check the data type of a column in a Pandas DataFrame?

ans-

```
print(type(df['name_of_column']))       # prints type of the specified column
or 
print(df.dtypes)          # prints data type of every column
```

Q3. How do you select rows from a Pandas DataFrame based on a condition?

ans -
```
print(df[(condition 1) & (condition 2)])
```

Q4. How do you rename columns in a Pandas DataFrame?

ans-
```
df.rename({'exisiting name':'new name'},axis='columns',inplace=True)  # the rename method uses a dict as a input to replace name of columns or index
```

Q5. How do you drop columns in a Pandas DataFrame?

ans- 

```
df.drop(['list of columns name to be dropped'],inplace=True)
```

Q6. How do you find the unique values in a column of a Pandas DataFrame?

ans- 
```
pd.unique(df['name of the column'])
```

Q7. How do you find the number of missing values in each column of a Pandas DataFrame?

ans-
```
df.isna().sum()        ## it isna function checks for all values of the dataframe that is null or not and the sum function counts the value of true.
```

Q8. How do you fill missing values in a Pandas DataFrame with a specific value?

ans-
```
df.fillna(value)  ## the fillna function contains argumnets like pad ,ffill and bfill to fill the null values
```

Q9. How do you concatenate two Pandas DataFrames?

ans-
```
final_df=pd.concat([df1,df2]) ## list of dataframes passed as a argument to the concat() function
```

Q10. How do you merge two Pandas DataFrames on a specific column?

ans-
```
final_df=df1.merge(df2,on='name of the column')
```

Q11. How do you group data in a Pandas DataFrame by a specific column and apply an aggregation function?

ans-
```
df.groupby('name of column by which group by is to be done ').count()
```

Q12. How do you pivot a Pandas DataFrame?

ans-
```
pivot_table=df.pivot_table(index=['column 1'] ,values=['column 2'] ,aggfunc={'mean','sum','median'})
```

Q13. How do you change the data type of a column in a Pandas DataFrame?

ans-
```
df['col name']=df['col name'].astype('data type')
```
Q14. How do you sort a Pandas DataFrame by a specific column?

ans-
```
sorted_df = df.sort_values(by='col name', ascending=False)
```
Q15. How do you create a copy of a Pandas DataFrame?

ans-
```
copied_df=df.copy()
```

Q16. How do you filter rows of a Pandas DataFrame by multiple conditions?

ans-
```
filtered_df=df[(condition 1) & (condition 2)]
```

Q17. How do you calculate the mean of a column in a Pandas DataFrame?

ans-
```
print(df['col name'].mean())
```

Q18. How do you calculate the standard deviation of a column in a Pandas DataFrame?

ans-
```
print(df['col name'].std())
```

Q19. How do you calculate the correlation between two columns in a Pandas DataFrame?

ans-
```
correlation=df.corr()['col name 1']['col name 2']
```

Q20. How do you select specific columns in a DataFrame using their labels?

ans-
```
final_df=df[['list of all columns you want to select']]
```

Q21. How do you select specific rows in a DataFrame using their indexes?

ans-
```
final_df=df.iloc[[list of specific row numbers ]]
```

Q22. How do you sort a DataFrame by a specific column?

ans-
```
sorted_df = df.sort_values(by='col name', ascending=False)
```

Q23. How do you create a new column in a DataFrame based on the values of another column?

ans-
```
df['new column']=df['old column']+any_value;
df['new column']=df['old column']-any_value;
df['new column']=df['old column']*any_value;
df['new column']=df['old column']/any_value;
```

Q24. How do you remove duplicates from a DataFrame?

ans-
```
duplicate_free_df=df.drop_duplicates()
```

Q25. What is the difference between .loc and .iloc in Pandas?

ans- .loc and .iloc both are used to slice the dataframe .The .loc method is uses the label based slicing and .iloc method uses index based slicing . while using the indexes in .loc method the second parameter is included in the slicing while in iloc method it is not included.

```
df.iloc[2:5]   ## it slices from 2nd row to 4th row
df.loc[2:5]    ## it slices from 2nd row to 5th row     
```   