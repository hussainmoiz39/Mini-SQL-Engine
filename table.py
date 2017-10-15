import sqlparse
import itertools
from prettytable import *
import os,sys



class Table():
	def __init__(self):
		self.Name = ''
		self.nofrows = 0        # n is no of rows in a table
		self.Attr = []          # stores name of cols
		self.Rows = []          # stores a row
		self.Cols = {}          # stores a col
		












