import sqlparse
import itertools
from prettytable import *
import os,sys
from engine import *
from query import *
from table import *

if __name__ == '__main__':
	mySql = SqlEngine()
	mySql.Engine()
