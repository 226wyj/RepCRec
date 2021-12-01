# RepCRec
This is the final project of NYU's Advanced Database Systems Course (CSCI-GA.2434-001), whose full name is `Replicated Concurrency Control and Recovery`.

### 1. What

This project is aim to implement a simple **distributed database**, complete with **multi-version concurrency control**, **deadlock detection**, **replication**, and **failure recovery**. 

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
* Each variable locks are acquired in a FCFS fashion.
* Use serialization graph when getting R/W locks.
* Use cycle detection to deal with deadlocks, abort the youngest transaction in the cycle (The system must keep track of the transaction time of any transaction holding a lock).
* Deadlock detection need not happen at every tick.
* read-only transactions should use multiversion read consistency.
  * If `xi` is not replicated and the site holding xi is up, then the read-only transaction can read it.
  * If `xi` if replicated then RO can read `xi` from site s if `si` was committed at s by some transaction T before RO began, and s was up all the time between the time when `xi` was commited and RO began.
  * To implement these, for every version of `xi`, on each site s, record when that version was committed. The transaction manager will record the filure history of every site.

## Test Specification
* input instructions come from a file or the standard input
* output goes to standard out
* If an operation for T1 is waiting for a lock held by T2, then when T2 commits, the operation for T1 proceeds if there is no other lock request ahead of it.
* 