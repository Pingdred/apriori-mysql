# MySQL Apriori

This is an implementation of the Apriori algorithm in MySQL, initially developed as part of a database project for the Databases exam at the University of Pisa. 

The Apriori algorithm is an algorithm for frequent ItemSet mining and association rule learning. It is used to discover frequent ItemSets from a transactional database and generate association rules based on the discovered ItemSets.

## Algorithm Steps

The Apriori algorithm implemented in this code follows these steps:

1. Extract the names of the items, these are the 1-ItemSets;
2. Calculate the support for each 1-ItemSet. Insert the frequent 1-ItemSet into the `Large_ItemSet_1` table.
3. For each ItemSet size from `k=2` to the maximum ItemSet size:
   - Generate the table `C` containing the candidate ItemSets.
   - Prune the candidate ItemSets by calculating their support and inserting the frequent ItemSets into a new table `Large_ItemSet_k`.
   - If the `Large_ItemSet_k` table is empty, to to the next step.
4. Calculate the confidence for each associative rule based on the frequent ItemSets in the last `Large_ItemSet_k`.

To learn more about the algorithm:
- [Wikipedia](https://en.wikipedia.org/wiki/Apriori_algorithm)
- [GeeksforGeeks](https://www.geeksforgeeks.org/apriori-algorithm/)
- [Original paper](https://www.vldb.org/conf/1994/P487.PDF)

## Transaction table structure

The transaction table must have the following format:

| ID| Item_1_name | Item_2_name | ... | Item_n_name |
|---|-------------|-------------|-----|-------------|
| 1 |       1     |      1      | ... |      0      |
| 2 |       0     |      1      | ... |      1      |
| 3 |       1     |      1      | ... |      0      |

In the repository, the file `Groceries_Dataset.sql` contains the [Groceries Dataset](https://www.kaggle.com/code/heeraldedhia/market-basket-analysis-using-apriori-algorithm/input). The procedure contained in the file `CreateTransactionTable.sql` allows you to generate the transaction table using the table containing the Groceries Dataset.


## Getting Started

1. Clone the repository:

   ```shell
   git clone https://github.com/sirius-0/apriori-mysql.git
   ```

2. Connect to your MySQL server using a client

3. Create a new database where you want to run the Apriori algorithm

4. Import the `Groceries_Dataset.sql`

5. Import the `CreateTransactiontable.sql`
 
6. Import the `Apriori.sql` 

7. Create the transaction table `T` running the `CreateTransactionTable` procedure

## Usage

To run the Apriori algorithm, use the following syntax:

```sql
CALL Apriori(transactionTableName, supportThreshold, ItemSetSize);
```

- `transactionTableName`: The name of the table containing the transaction data. The table should have one column for each item and a row for each transaction.
- `supportThreshold`: The minimum support threshold for an ItemSet to be considered frequent. It should be a number between 0 and 1.
- `ItemSetSize`: The maximum size of the ItemSets to be generated.

Example:

```sql
CALL Apriori('T', 0.5, 3);
```

This will run the Apriori algorithm on the `transactions` table with a support threshold of 0.5 and generate ItemSets up to size 3.

## Final notes

This implementation is not optimized and is extremely slow.

Introducing indexes on the transaction table and `Large_ItemSet_k` tables could speed up the generation of candidate ItemSets and the support calculation, but introducing indexes has some problems:
- InnoDB supports up to 64 secondary indexes per table, which might not be enough if the number of Items is too high;
- You could dynamically add and drop indexes while executing the `Apriori` procedure but modifying the information schema is onerous and would perhaps affect performance more than the introduction of indexes improves it;

To solve the indexing problem, one could switch to a different representation of the transaction table, such as the [Compressed Sparse Row](https://en.wikipedia.org/wiki/Sparse_matrix#Compressed_sparse_row_(CSR,_CRS_or_Yale_format)) representation.
