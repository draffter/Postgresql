#-*- coding: utf-8 -*-
import sublime, sublime_plugin
import pg8000
import os
import json
import fnmatch
import shutil
from collections import namedtuple

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
		try:
			if self.settings.get('show_confirm') == True:
				if sublime.ok_cancel_dialog("You are using '"+self.connectionName+"' connection.\nContinue?"):
					self.cursor.execute(sql.decode('utf-8'))
					self.connection.commit()
				else:
					return None
			else:
				self.cursor.execute(sql.decode('utf-8'))
				self.connection.commit()
		except Exception, e:
			self.disconnect()
			s = "Error {0}".format(str(e)) # string
			utf8str = s.encode("utf-8")
			self.showResult('Error in SQL' + utf8str + "\nSQL:\n\t'" + sql.decode('utf-8') +"'", False)
			return None
		else:
			if self.cursor.rowcount > 0 :
				try:
					rows = self.cursor.fetchall()
					return rows
				except Exception, e:
					pass
		return False

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
		view.set_name('PG result for '+self.connectionName)
		edit = view.begin_edit()
		if isOk:
			pResult = self.prepareOutView(result)
		else:
			pResult = result
		view.insert(edit, 0, pResult.decode('utf-8'))
		view.end_edit(edit)

	# przygotowanie tabeli
	def prepareOutView(self, result):
		data = []
		cNames = self.getColumnNames()
		RowTable = namedtuple('RowTable',cNames)
		ret = ''
		for row in result:
			sRow = []
			for col in row:
				if col == None:
					sRow.append(' ')
				else:
					# print type(col)
					# if isinstance(col, unicode):
					# 	print col.encode('utf-8')
					# else:
					# 	print col
					# print "+++++++++++++++"
					if isinstance(col, unicode):
						sRow.append(col.encode('utf-8'))
					elif isinstance(col, str):
						sRow.append(col)
					else:
						sRow.append(str(col).encode('utf-8'))
			data.append(RowTable(*sRow))
		ret = self.pprinttable(data)
		return ret

	#rysowanie tabeli
	def pprinttable(self,rows):
		out = ''
		if len(rows) > 0:
			headers = rows[0]._fields
			lens = []
			for i in range(len(rows[0])):		#szerokosc kolumn
				lens.append(len(max([x[i] for x in rows] + [headers[i]],key=lambda x:len(str(x)))))
			formats = []
			hformats = []
			for i in range(len(rows[0])):
				if isinstance(rows[0][i], int):
					formats.append("%%%dd" % lens[i])
				else:
					formats.append("%%-%ds" % lens[i])
				hformats.append("%%-%ds" % lens[i])
			pattern = " | ".join(formats).encode('utf-8')
			hpattern = " | ".join(hformats)
			separator = "-+-".join(['-' * n for n in lens])
			out += hpattern % tuple(headers)
			out += "\n"
			out += separator
			out += "\n"
			for line in rows:
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

