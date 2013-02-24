#-*- coding: utf-8 -*-
import sublime, sublime_plugin
import pg8000
import os
import json
import fnmatch
from collections import namedtuple


class PostgresqlCommand(sublime_plugin.TextCommand):
	settingsPath = ''
	connection = None       #ConnectionWrapper
	cursor = None           #CursorWrapper
	connParams = None
	e = None
	cFiles = []

	def showErrorMessage(self, mgs):
		if self.e != None:
			s = "Error {0}".format(str(self.e)) # string
			utf8str = s.encode("utf-8")
		else:
			utf8str = "";
		sublime.error_message("Could not get preferences\n" + utf8str)

	def run(self, view):
		self.cFiles = []
		res = self.getMainConnection()
		return
		if res == False:
			self.showErrorMessage('Could not get preferences')
			return
		
		res = self.connect()
		if res == False:
			self.showErrorMessage('Could not connect to database')
			return
		print "done"
		# sel = self.view.sel()[0]
		# if sel.empty():
		# 	selection = self.view.substr(sublime.Region(0, self.view.size()))
		# else:
		# 	selection = self.view.substr(self.view.sel()[0])
		# self.connect()
		# result = self.execute(selection)
		# if (result != None):
		# 	self.showResult(result, True)
		# 	self.disconnect()

	def getMainConnection(self):
		settings = sublime.load_settings('postgresql.sublime-settings')
		cc = settings.get('current_connection')
		print cc
		if cc == None:
			return self.getConnections()
		else:
			return self.setConnectionParams(cc)

	# zapisuje nowe bieżące połączenie
	def saveNewMainConnection(self, num):
		settings = sublime.load_settings('postgresql.sublime-settings')
		settings.set('current_connection', self.cFiles[num])
		sublime.save_settings('postgresql.sublime-settings')

	# pobiera parametry połączenia
	def setConnectionParams(self, cc):
		connParamsPath = os.path.join(os.getcwd(), 'Connections', cc + '.settings')
		with open(connParamsPath,'r') as f:
			try:
				self.connParams = json.load(f)
				return True
			except Exception, e:
				raise e 	#TODO
			finally:
				f.close()

	# pobiera listę możliwych połączeń
	def getConnections(self):
		cPath = os.path.join(os.getcwd(), 'Connections')		
		#os.getcwd() się nie sprawdza, najlepiej uzyć w settings wpisu, gdzie zapisywać connections
		# domyślnie w packages_path()/pgconnections
		print cPath
		for path, dirs,files in os.walk(cPath):
			for filename in fnmatch.filter(files, '*.settings'):
				self.cFiles.append(filename[:-9]);
		print self.cFiles
		if len(self.cFiles) > 0:
			self.view.window().show_quick_panel(self.cFiles, self.saveNewMainConnection)
			return True
		else:
			return False

	# otwiera plik z konfiguracją połączenia
	def openPrefConnFile(self, num):
		if num > -1:
			path = os.path.join(os.getcwd(), 'Connections', self.cFiles[num] + '.settings')
			self.view.window().open_file(path)


	def connect(self):
		# self.connection = pg8000.dbapi.connect(user='sn0', host='localhost', database='postgres', password='haslo123', socket_timeout=5)
		try:
			self.connection = pg8000.dbapi.connect(user=str(self.connParams['user']),port=self.connParams['port'], host=str(self.connParams['host']), database=str(self.connParams['database']), password=str(self.connParams['password']), socket_timeout=self.connParams['socket_timeout'])
		except Exception, self.e:
			return False
		self.connection.autocommit = True
		self.cursor = self.connection.cursor()

	def execute(self, sql):
		try:
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
		view.set_name('PG result')
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
					if isinstance(col, int):
						sRow.append(str(col).encode('utf-8'))
					else:
						sRow.append(col.encode('utf-8'))
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
<<<<<<< HEAD

=======
>>>>>>> 0d97cc6bcb3d29af4b0e88659ab87bfedcd7c778
