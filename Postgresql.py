#-*- coding: utf-8 -*-
import sublime, sublime_plugin
import pg8000
import os
import datetime
import json
import fnmatch
import shutil
from collections import namedtuple
import collections


class ConnectionchangeCommand(PostgresqlCommand):
	settings = None
	def run(self, view):
		self.settings = sublime.load_settings('Postgresql.sublime-settings')
		self.getConnections()

class EditconnectionCommand(PostgresqlCommand):
	settings = None
	def run(self, view):
		self.openPrefConnFile()
	# otwiera plik z konfiguracją połączenia
	def openPrefConnFile(self):
		default_settings_path = os.path.join(sublime.packages_path(), 'Postgresql', 'Postgresql.sublime-settings')
		sublime.active_window().open_file(default_settings_path)

class PostgresqlCommand(sublime_plugin.TextCommand):
	settingsPath = ''
	connection = None       #ConnectionWrapper
	cursor = None           #CursorWrapper
	connParams = None
	settings = None
	e = None
	connectionName = None
	cFiles = []

	def showErrorMessage(self, msg):
		if self.e != None:
			s = "Error {0}".format(str(self.e)) # string
			utf8str = s.encode("utf-8")
		else:
			utf8str = "";
		sublime.error_message(msg + "\n" + utf8str)

	def run(self, view):
		self.cFiles = []
		res = self.getMainConnection()
		if res ==  False:
			print "false"
		else:
			print "true"
		if res == False:
			return

		resc = self.connect()
		if resc == False:
			self.showErrorMessage('Could not connect to database')
			return
		sel = self.view.sel()[0]
		if sel.empty():
			selection = self.view.substr(sublime.Region(0, self.view.size()))
		else:
			selection = self.view.substr(self.view.sel()[0])
		self.connect()
		result = self.execute(selection)
		if (result != None):
			self.showResult(result, True)
			self.disconnect()

	# pobiera obecnie wybrane połącznenie
	def getMainConnection(self):
		self.settings = sublime.load_settings('Postgresql.sublime-settings')
		cc = self.settings.get('current_connection')
		if cc == None or cc == "":
			return self.getConnections()
		else:
			self.connectionName = cc
			return self.setConnectionParams(cc)

	# zapisuje nowe bieżące połączenie
	def saveNewMainConnection(self, num):
		self.settings.set('current_connection', self.cFiles[num])
		sublime.save_settings('Postgresql.sublime-settings')

	# pobiera parametry połączenia
	def setConnectionParams(self, cc):
		c = self.settings.get('connections')
		self.connParams = c[cc]
		return True

	# pobiera listę możliwych połączeń
	def getConnections(self):
		self.cFiles = []
		c = self.settings.get('connections')
		for key in c:
			self.cFiles.append(key)
		self.view.window().show_quick_panel(self.cFiles, self.saveNewMainConnection)
		return False

	def connect(self):
		try:
			self.connection = pg8000.dbapi.connect(user=str(self.connParams['user']),port=self.connParams['port'], host=str(self.connParams['host']), database=str(self.connParams['database']), password=str(self.connParams['password']), socket_timeout=self.connParams['socket_timeout'])
		except Exception, self.e:
			return False
		self.connection.autocommit = True
		self.cursor = self.connection.cursor()

	def execute(self, sql):
		if 'schema' in self.connParams and self.connParams['schema'] != "":
			self.cursor.execute("set search_path = '"+ self.connParams['schema'] +"'")
		sql = sql.replace('%','%%')
		try:
			if self.settings.get('show_confirm') == True or ('warn' in self.connParams and self.connParams['warn'] == True):
				if sublime.ok_cancel_dialog("You are using '"+self.connectionName+"' connection.\nContinue?"):
					self.cursor.execute(sql)
					self.connection.commit()
				else:
					return None
			else:
				self.cursor.execute(sql)
				self.connection.commit()
		except Exception, e:
			self.disconnect()
			s = "Error {0}".format(str(e).decode("utf-8")) # string
			utf8str = s.encode("utf-8")
			self.showResult('Error in SQL' + utf8str + "\nSQL:\n\t'" + sql.encode('utf-8') +"'", False)
			return None
		else:
			if self.cursor.rowcount > 0 :
				try:
					rows = self.cursor.fetchall()
					return rows
				except Exception, e:
					pass
		return False

	# wywoływana, gdy w pierszwym wierszu wyników nie ma danych
	# w kolumnie, do określenia typu sprawdza zawartość kolumny
	# dla kolejnych wierszy
	def searchType(self, result, i):
		for row in result:
			t = self.convertTypes(type(row[i]))
			if t != 'unknown':
				return t
		return t

	# pobiera typy kolumn
	def getColumnsTypes(self, result):
		cTypes = []
		for i, col in enumerate(result[0]):
			newType = self.convertTypes(type(col))
			if newType == 'unknown':
				newType = self.searchType(result, i)
			cTypes.append(str('<' + newType + '>'))
		return cTypes

	# pobiera nazwy kolumn
	def getColumnNames(self):
		cNames = []
		for c in self.cursor.description:
			cNames.append(c[0])
		return cNames

	def disconnect(self):
		self.cursor.close()
		self.connection.close()

	# wyświetlenie rezultatu w nowym tabie
	def showResult(self, result, isOk):
		if result == False:
			sublime.message_dialog('Query returns 0 rows')
			return

		view = sublime.active_window().new_file()
		view.settings().set('word_wrap', False)
		view.set_name('PG result for '+self.connectionName)
		edit = view.begin_edit()
		if isOk:
			pResult = self.prepareOutView(result)
		else:
			pResult = result
		view.insert(edit, 0, pResult.decode('utf-8'))
		view.end_edit(edit)

	# dodaje do nazwy kolumny "_n" aby nie było problemów z listą
	def prepareColumnNames(self, cName):
		nName = []
		cnt = collections.defaultdict(list)
		for name in cName:
			if name in cnt:
				cnt[name] += 1
			else:
				cnt[name] = 1
			if cnt[name] > 1:
				_name = name + "_"+str((cnt[name]-1))
				name = _name
			nName.append(name.replace(" ","_"))
		return nName

	# dekodowanie microsekund na interwał
	def intervalToHis(self, microseconds):
		s=microseconds/1000000
		i,s=divmod(s,60)
		h,i=divmod(i,60)
		return "%02d:%02d:%02d" % (h,i,s)

	# przygotowanie tabeli
	def prepareOutView(self, result):
		data = []
		cNames = self.getColumnNames()
		cTypes = self.getColumnsTypes(result)
		RowTable = namedtuple('RowTable',self.prepareColumnNames(cNames))
		data.append(RowTable(*cTypes))
		ret = ''
		for row in result:
			sRow = []
			for col in row:
				if col == None:
					sRow.append(' ')
				else:
					if isinstance(col, str):
						sRow.append(col)
					elif isinstance(col, pg8000.types.Interval):
						interval = self.intervalToHis(col.microseconds)
						sRow.append(interval.encode('utf-8'))
					elif isinstance(col, unicode):
						sRow.append(col.encode('utf-8'))
					else:
						sRow.append(str(col).encode('utf-8'))
			data.append(RowTable(*sRow))
		ret = self.pprinttable(data)
		return ret

	#rysowanie tabeli
	def pprinttable(self,rows):
		out = ''
		out += ("Result for connection: " + self.connectionName +"\n").encode('utf-8')
		out += ("Affected rows: " + str(self.cursor.rowcount) + "\n\n").encode('utf-8')
		if len(rows) > 0:
			headers = rows[0]._fields
			types = rows[0]
			lens = []
			for i in range(len(rows[0])):       #szerokosc kolumn
				lens.append(len(max([x[i] for x in rows] + [headers[i]],key=lambda x:len(str(x)))))
			formats = []
			hformats = []
			for i in range(len(rows[0])):
				if isinstance(rows[0][i], int):
					formats.append("%%%dd" % lens[i])
				else:
					formats.append("%%-%ds" % lens[i])
				hformats.append("%%-%ds" % lens[i])
			hpattern = " | ".join(hformats)
			separator = "-+-".join(['-' * n for n in lens])
			out += hpattern % tuple(headers)
			out += "\n"
			for cnt, line in enumerate(rows):
				if cnt == 0:
					out += hpattern % tuple(types)
					out += "\n"
					out += separator
					out += "\n"
				else:
					i =0
					for col in line:
						lgh = len(col.decode('utf-8'))
						out += col
						for space in range(lgh, lens[i]):
							out += ' '
						i = i+1
						if i != len(line):
							out += ' | '
					out += "\n"
		return out

	def convertTypes(self, typeIn):
		orgType = typeIn
		if '<type' in str(typeIn):
			typeIn = str(typeIn).replace("<type ", '').replace("'","").replace(">","")
		if '<class' in str(typeIn):
			typeIn = str(typeIn).replace("<class ", '').replace("'","").replace(">","")

		pgtypes = {
			'bool':'bool',
			'int':'int4',
			'long':'numeric',
			'str':'text',
			'unicode':'text',
			'float':'float8',
			'decimal.Decimal':'numeric',
			'pg8000.types.Bytea':'bytea',
			'datetime.datetime (wo/ tzinfo)':'timestamp without time zone',
			'datetime.datetime (w/ tzinfo)':'timestamp with time zone',
			'datetime.date':'date',
			'datetime.datetime':'time without time zone',
			'datetime.time':'time without time zone',
			'pg8000.types.Interval':'interval',
			'None':'NULL',
			'list of int':'INT4[]',
			'list of float':'FLOAT8[]',
			'list of bool':'BOOL[]',
			'list of str':'TEXT[]',
			'list of unicode':'TEXT[]',
			'NoneType':'unknown'
		}

		if typeIn in pgtypes:
			return pgtypes[typeIn]

		return orgType