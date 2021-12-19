# RepCRec

This is the final project of NYU's Advanced Database Systems Course (CSCI-GA.2434-001), whose full name
is `Replicated Concurrency Control and Recovery`.

## What

This project is aim to implement a simple **distributed database model**, complete with **multi-version concurrency
control**
, **deadlock detection**, **replication**, and **failure recovery**.


## How to run

1. Please make sure that you have `Python 3.6+` installed.
2. To get input from file, use the following command:

    ```shell
    python main.py --file
    ```

   Then, the CLI will output the guide message as shown below:

   ```
   Please input file path:
   > 
   ```

   Just type in the file name after `>`, and make sure to add the relative path to `main.py`, which means, suppose the
   file name is `test1`, and it is in the `test` directory relative to `main.py`, then we should type in `test/test1`
   after `>`.

   After finishing processing the given file, the CLI will output another guide message to ask whether you want to
   process another file, the guide message is shown below:

   ```
   Continue[y/n]?
   ```
    
   If you want to continue getting inputs from another file, please type in `y`, then the hint message that guides you
   to input file name will show again. Otherwise, please input `n`, and the program will exit.

3. To get input from standard input, use the following command:

   ```shell
   python main.py --std 
   ```

   Then, the CLI will output the guide message as shown below:

   ```
   Standard input, use 'exit' to exit.
   > 
   ```

   and you can type in the command after `>`. To exit the `--std` mode, just type in `exit` after `>`.

4. To make it easier to test multiple files at the same time, I also add a `--dir` option (although it is not required).
   To get input files from a target directory, use the following command:

   ```shell
   python main.py --dir
   ``` 

   Then, the CLI will output the following guide message to let you type in the target directory:

   ```
   Please input the root directory: 
   > 
   ```

   After got the directory, the program will iterate all the files in the directory and process every one of them. For
   example, suppose all the test files are in the relative directory to `main.py` whose name is `test`, then all you
   need to input after `>` is `test`, and the program will process all the test files in `test` directory automatically.

5. If you get stuck, you can get help with `-h` option at any time. For example, use

    ```shell
    python main.py -h
    ```
   
    and the brief introduction of different arguments will be printed:

    ```text
    usage: main.py [-h] [--file] [--std] [--dir]
    
    Choose whether to get input from the keyboard or the file
    
    optional arguments:
      -h, --help  show this help message and exit
      --file      whether to get input from file
      --std       whether to get input from standard input
      --dir       whether to get input from a directory.
    ```
   
    The most important thing is, be sure to give one and only one right argument when using the program.

## Data

* Data consists of 20 distinct variables (from `x1` to `x20`)
* There are 10 sites numbered 1 to 10.
* A copy is indicated by a dot, `x6.2` means the copy of variable `x6` at site 2.
* The odd indexed variables are at one site each `1 + (index number mod 10)`
* Even indexed variables are at all sites.
* Each variable `xi` is initialized to the value `10 * i`
* Each site has an independent lock table, if the site fails, the lock table is erased.

## Algorithms

* Use strict two phase locking (read and write locks) at each site.
* Validation at commit time.
* A transaction may read a variable and later write that same variable as well as others, use lock promotion.
* Available copies allows writes and commits to just the available sites.
* Each variable locks are acquired in a `FCFS` fashion.
* Use serialization graph when getting R/W locks.
* Use cycle detection to deal with deadlocks, abort the youngest transaction in the cycle (The system must keep track of
  the transaction time of any transaction holding a lock).
* Deadlock detection uses cycle detection and will abort the youngest transaction in the cycle. It need not happen at
  every tick, but when it does, it should happen at the beginning of the tick.
* read-only transactions should use multi-version read consistency.
    * If `xi` is not replicated and the site holding xi is up, then the read-only transaction can read it.
    * If `xi` is replicated then RO can read `xi` from site `s` if `si` was committed at `s` by some transaction T before
      RO began, and `s` was up all the time between the time when `xi` was committed and RO began.
    * To implement these, for every version of `xi`, on each site s, record when that version was committed. The
      transaction manager will record the failure history of every site.

## Test Specification

* input instructions come from a file or the standard input
* output goes to standard out
* If an operation for T1 is waiting for a lock held by T2, then when T2 commits, the operation for T1 proceeds if there
  is no other lock request ahead of it.
* Lock acquisition is `FCFS` but several transactions may hold read locks on the same item(share read locks). For example:
    * If `x` is currently read-locked by `T1` and there is no waiting list and `T2` requests a read lock on `x`,
      then `T2` can get it.
    * If `x` is read-locked by `T1` and `T3` is waiting for a write lock on `x` and `T2` subsequently requests a read
      lock on `x`, then `T2` must wait for `T3` either to be aborted or to complete its possession of `x`.
* W(T1, x6, v) means that transaction `T1` wants to write all available copies of `x6` with the value `v`, and it can
  happen only when `T1` has locks on all sites that are up and that contain `x6`. If `T1` can only get some locks but
  not all, then `T1` should release those locks finally.
* fail(6): test command, means site 6 fails.
* recover(7): test command, means site 7 recovers.

## Print Format

* R(T1, x4) => x4: 5
* dump() => site 1 - x2: 6, x3: 2, ..., x20: 3, one line per site, includes sites that are down.
* end(T1): The system should report whether T1 can commit in the format `T1 commits` or `T1 aborts`
* When a transaction commits, the name of the transaction should be printed.
* When a transaction aborts, the name of the transaction should be printed.
* When sites are affected by a `write` transaction, the site's name should be printed.
* Every time a transaction waits because of a lock conflict, the transaction's name and the reason should be printed.
* Every time a transaction waits because a site is down, the transaction's name and the reason should be printed.
			  