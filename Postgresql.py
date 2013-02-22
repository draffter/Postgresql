#-*- coding: utf-8 -*-
import sublime, sublime_plugin
import pg8000
from collections import namedtuple


class PostgresqlCommand(sublime_plugin.TextCommand):
	connection = None       #ConnectionWrapper
	cursor = None           #CursorWrapper

	def run(self, edit):
		sel = self.view.sel()[0]
		if sel.empty():
			selection = self.view.substr(sublime.Region(0, self.view.size()))
		else:
			selection = self.view.substr(self.view.sel()[0])
		self.connect()
		result = self.execute(selection)
		self.showResult(result)
		self.disconnect()

	def connect(self):
		self.connection = pg8000.dbapi.connect(user='sn0', host='192.168.16.135', database='pwi2_devel', password='n0wysl4w3k!', socket_timeout=5)

	def execute(self, sql):
		self.cursor = self.connection.cursor()
		try:
			self.cursor.execute(sql.decode('utf-8'))
		except Exception, e:
			sublime.error_message('Error in SQL')

		if self.cursor.rowcount == 0 :
			return False
		else:
			rows = self.cursor.fetchall()
		return rows

	def getColumnNames(self):
		cNames = []
		for c in self.cursor.description:
			cNames.append(c[0])
		return cNames

	def disconnect(self):
		self.cursor.close()
		self.connection.close()

	# wyświetlenie rezultatu w nowym tabie
	def showResult(self, result):
		if result == False:
			sublime.message_dialog('No data found')
			return

		view = sublime.active_window().new_file()
		view.set_name('PG result')
		edit = view.begin_edit()
		pResult = self.prepareOutView(result)
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