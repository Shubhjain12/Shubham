import numpy as np
import pandas as pd
import os
import sys

from connection_master import (BASE_URL, connection_string, database_name,
                               endpoint, file_path, live_connection_string,
                               live_database_name, live_endpoint,
                               live_password, live_username, password,
                               read_connection_string, s3, s3_feedback_path,
                               s3_user_file_path, s3_report_path, session, username)


class CheckValidity:
	"""
	This class is designed to perform various validity checks standard to bulk upload.
	Pandas dataframe is used as the data structure.
	The class can be initialized using the original dataframe as the argument for the constructor.
	"""
	def __init__(self, dataframe):
		self.__dataframe = dataframe
		self.__valid = dataframe
		self.__invalid = []

	def getOriginalDf(self):
		"Returns the original data used during initialization"
		return self.__dataframe

	def getValidDf(self):
		"Returns the currently valid data"
		return self.__valid

	def getInvalidDf(self):
		"Returns the currently invalid data"
		return pd.concat(self.__invalid, ignore_index=True)

	def print_invalid(self):
		"Returns the list of invalid dataframe along with the size for each one of them"
		i = 0
		for x in self.__invalid:
			print("Invalid ", i, "size:", x.shape[0])
			i += 1

	def removeDuplicateSelf(self, criterialist:list, remark:str, keepfirst:bool=False, dedupcriterialist:list=[], identify_duplicate:str=None, allow_duplicate:bool=False):
		"""
		Remove duplicate data from itself using a defined set of criteria

		:param list criterialist: list of columns to consider for duplicate check
		:param str remark: remark to add in case of duplicate data
		:param bool keepfirst: flag to decide whether to keep the first of exact duplicate entries
		:param list dedupcriterialist: list of columns to consider for identifying exact duplicate entries
		:param str identify_duplicate: column to use to identify groups of duplicate entries and add remark accordingly
		:param bool allow_duplicate: whether to allow duplicate entry via 'DUPLICATE' column or strictly detect duplicate entries
		"""
		print("\nIn removeDuplicateSelf:\ncriterialist:", criterialist,
			  " keepfirst:", keepfirst, " dedupcriterialist:", dedupcriterialist)
		print("valid:", self.__valid.shape[0])
		self.print_invalid()
		if self.__valid.empty:
			print("no valid data to check")
			return True
		if 'DUPLICATE' in self.__valid.columns:
			i = self.__valid[self.__valid['DUPLICATE'] == 'CHECKED'].shape[0]
			if i > 0:
				keepfirst = False

		try:
			self.__valid["DUP_ID"] = self.__valid.duplicated(
				subset=criterialist, keep=False)

			if ('DUPLICATE' in self.__valid.columns) and (allow_duplicate):
				conditions2 = [
					((self.__valid['DUP_ID'] == True) &
					 (self.__valid['DUPLICATE'] != 'CHECKED')),
					((self.__valid['DUP_ID'] == False) |
					 (self.__valid['DUPLICATE'] == 'CHECKED'))
				]
			else:
				conditions2 = [
					(self.__valid['DUP_ID'] == True),
					(self.__valid['DUP_ID'] == False)
				]

			values3 = [
				remark,
				''
			]
			values4 = [0, 1]

			self.__valid['REMARK'] = np.select(conditions2, values3)
			self.__valid['BUSINESS_LOGIC_VALID'] = np.select(
				conditions2, values4)

			dup_entries_id = (self.__valid[self.__valid['BUSINESS_LOGIC_VALID'] == 0]).drop(
				['DUP_ID'], axis=1, errors='ignore')
			self.__valid = self.__valid[self.__valid['BUSINESS_LOGIC_VALID'] == 1]

			if keepfirst:
				exec_status = self.keepFirst(
					dup_entries_id, criterialist, dedupcriterialist, identify_duplicate)
				if exec_status != True:
					return exec_status
			else:
				if identify_duplicate is None:
					self.__invalid.append(dup_entries_id)
				else:
					exec_status = self.identifyDuplicate(
						dup_entries_id, identify_duplicate, criterialist)
					if exec_status != True:
						return exec_status

			exec_status = self.removeColumns(['DUP_ID'])
			if exec_status != True:
				return exec_status
			print("valid:", self.__valid.shape[0])
			self.print_invalid()
			print("End of removeDuplicateSelf\n")
			return True

		except Exception as e:
			print("Exception in dfvaliditycheck")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e

	def identifyDuplicate(self, dup_entries_id:pd.DataFrame, identifier:str, criterialist:list):
		"""
		Identify sets of entries that are duplicates among themselves using specific column

		:param dataframe dup_entries_id: dataset consisting of all duplicate entries
		:param str identifier: column name based on which duplicate set to be identified
		:param list criterialist: list of columns to consider for duplicate check
		"""
		print("In identifyDuplicate,\ncriterialist: ",
			  criterialist, " identifier: ", identifier)
		print("valid:", self.__valid.shape[0])
		self.print_invalid()
		if dup_entries_id.empty:
			print("no duplicate entries for identification")
			return True
		temp_invalid = pd.DataFrame()
		# print("dup_entries_id headers: ", dup_entries_id.columns)
		try:
			grouped = list(dup_entries_id.groupby(criterialist))
			for i in range(len(grouped)):
				temp_group = grouped[i][1]
				identifier_list = temp_group[identifier].fillna(
					value='').unique().tolist()
				s = ','.join(identifier_list)
				temp_group['REMARK'] = temp_group['REMARK'] + str(s)
				temp_invalid = pd.concat(
					[temp_invalid, temp_group], ignore_index=True)

			self.__invalid.append(temp_invalid)
			print("valid:", self.__valid.shape[0])
			self.print_invalid()
			print("End of identifyDuplicate\n")
			return True

		except Exception as e:
			print("Exception in dfvaliditycheck")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e

	def removeDuplicateFromDf(self, dataframe2:pd.DataFrame, criterialist:list, remark:str, identify_duplicate:str=None, allow_duplicate:bool=False):
		"""
		Remove duplicate data after comparing with another dataframe using a defined set of criteria

		:param dataframe dataframe2: the second dataframe used for duplicate identification
		:param list criterialist: list of columns to consider for duplicate check
		:param str remark: remark to add in case of duplicate data
		:param str identify_duplicate: column to use to identify groups of duplicate entries and add remark accordingly
		:param bool allow_duplicate: whether to allow duplicate entry via 'DUPLICATE' column or strictly detect duplicate entries
		"""
		print("\nIn removeDuplicateFromDF,\ncriterialist: ", criterialist)
		print("valid:", self.__valid.shape[0])
		self.print_invalid()
		if self.__valid.empty:
			print("no valid data to check")
			return True
		if dataframe2.empty:
			print("no second dataset to check from")
			return True
		try:
			if identify_duplicate is None:
				self.__valid = pd.merge(
					self.__valid, dataframe2[criterialist], on=criterialist, how='left', indicator='DUP_ID_DB_COLUMNS')
			else:
				self.__valid = pd.merge(self.__valid, dataframe2[criterialist + [
									  identify_duplicate]], on=criterialist, how='left', indicator='DUP_ID_DB_COLUMNS')
			self.__valid['DUP_ID_DB_COLUMNS'] = np.where(
				self.__valid.DUP_ID_DB_COLUMNS == 'both', True, False)

			if ('DUPLICATE' in self.__valid.columns) and (allow_duplicate):
				conditions = [
					((self.__valid['DUP_ID_DB_COLUMNS'] == True)
					 & (self.__valid['DUPLICATE'] != 'CHECKED')),
					((self.__valid['DUP_ID_DB_COLUMNS'] == False)
					 | (self.__valid['DUPLICATE'] == 'CHECKED'))
				]
			else:
				conditions = [
					(self.__valid['DUP_ID_DB_COLUMNS'] == True),
					(self.__valid['DUP_ID_DB_COLUMNS'] == False)
				]

			values = [remark,
					  '']
			values2 = [0, 1]

			self.__valid['REMARK'] = np.select(conditions, values)
			self.__valid['BUSINESS_LOGIC_VALID'] = np.select(conditions, values2)

			dup_df = (self.__valid[self.__valid['BUSINESS_LOGIC_VALID'] == 0]).drop(
				['DUP_ID_DB_COLUMNS'], axis=1, errors='ignore')

			if identify_duplicate is not None:
				dup_df['REMARK'] = dup_df['REMARK'] + \
					dup_df[identify_duplicate]
				dup_df.drop([identify_duplicate], axis=1, errors='ignore')

			self.__invalid.append(dup_df)

			self.__valid = self.__valid[self.__valid['BUSINESS_LOGIC_VALID'] == 1]

			exec_status = self.removeColumns(
				['DUP_ID_DB_COLUMNS', identify_duplicate])
			if exec_status != True:
				return exec_status
			print("valid:", self.__valid.shape[0])
			self.print_invalid()
			print("End of removeDuplicateFromDf\n")
			return True

		except Exception as e:
			print("Exception in dfvaliditycheck")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e

	def matchFromDf(self, dataframe2:pd.DataFrame, criterialist:list, remark:str):
		"""
		Remove entries that don't match certain criteria with another dataframe

		:param dataframe dataframe2: the second dataframe used for matching
		:param list criterialist: list of columns to consider for matching
		:param str remark: remark to add in case of unmatched data
		"""
		print("\nIn matchFromDf,\ncriterialist: ", criterialist)
		print("valid:", self.__valid.shape[0])
		# print(dataframe2.info())
		self.print_invalid()
		if self.__valid.empty:
			print("no valid data to check")
			return True
		try:
			# self.__valid.to_csv(file_path + "endorsement_valid.csv", index=False)
			# dataframe2.to_csv(file_path + "endorsement_df2.csv", index=False)
# <<<<<<< HEAD
# 			self.valid = pd.merge(
# 				self.valid, dataframe2, on=criterialist, how='left', indicator='MATCHED_COLUMNS')
# 			self.valid['MATCHED_COLUMNS'] = np.where(
# 				self.valid.MATCHED_COLUMNS == 'both', True, False)
# =======
			self.__valid = pd.merge(
				self.__valid, dataframe2, on=criterialist, how='left', indicator='MATCHED_COLUMNS')
			self.__valid['MATCHED_COLUMNS'] = np.where(
				self.__valid.MATCHED_COLUMNS == 'both', True, False)

# >>>>>>> f6257969c9e482a382bc47531a53a7fbe873cdc2
			conditions = [
				(self.__valid['MATCHED_COLUMNS'] == False),
				(self.__valid['MATCHED_COLUMNS'] == True)
			]

			values = [remark,
					  '']
			values2 = [0, 1]

			self.__valid['REMARK'] = np.select(conditions, values)
			self.__valid['BUSINESS_LOGIC_VALID'] = np.select(conditions, values2)

			self.__invalid.append((self.__valid[self.__valid['BUSINESS_LOGIC_VALID'] == 0]).drop(
				['MATCHED_COLUMNS'], axis=1, errors='ignore'))

			self.__valid = self.__valid[self.__valid['BUSINESS_LOGIC_VALID'] == 1]
			exec_status = self.removeColumns(['MATCHED_COLUMNS'])
			if exec_status != True:
				return exec_status
			print("valid:", self.__valid.shape[0])
			self.print_invalid()
			print("End of matchFromDf\n")
			return True

		except Exception as e:
			print("Exception in dfvaliditycheck")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e

	def checkPositiveNumeric(self, column_to_be_checked:list, decimal:bool=True, column_value_to_be_assigned:dict={}):
		"""
		Check if a column value is positive decimal/integer

		:param list column_to_be_checked: list of columns to be checked
		:param bool decimal: flag for decimal/integer check
		:param dict column_value_to_be_assigned: dictionary to facilitate update in any column other than BUSINESS_LOGIC_VALID
		"""
		print("\nIn checkPositiveNumeric,\ncolumn_to_be_checked: ", column_to_be_checked,
			  " column_value_to_be_assigned: ", column_value_to_be_assigned)
		print("valid:", self.__valid.shape[0])
		self.print_invalid()
		if self.__valid.empty:
			print("no valid data to check")
			return True
		try:
			column_value_to_be_assigned.update({'BUSINESS_LOGIC_VALID': 0})

			for i in column_to_be_checked:
				if decimal == True:
					self.__valid.loc[self.__valid[i].notnull() & (pd.to_numeric(self.__valid[i], errors='coerce').isna() | self.__valid[i].str.contains(
						'-', na=False, regex=True)), 'REMARK'] = self.__valid['REMARK']+i+' is not a valid number'+' , '
					self.__valid.loc[self.__valid[i].notnull() & (pd.to_numeric(self.__valid[i], errors='coerce').isna() | self.__valid[i].str.contains(
						'-', na=False, regex=True)), list(column_value_to_be_assigned.keys())] = list(column_value_to_be_assigned.values())
				else:
					self.__valid.loc[(self.__valid[i].notnull()) & ~(self.__valid[i].str.isdigit().fillna(
						value=True)), 'REMARK'] = self.__valid['REMARK']+i+' is not a valid number'+' , '
					self.__valid.loc[(self.__valid[i].notnull()) & ~(self.__valid[i].str.isdigit().fillna(value=True)), list(
						column_value_to_be_assigned.keys())] = list(column_value_to_be_assigned.values())
			self.__invalid.append(
				(self.__valid[self.__valid['BUSINESS_LOGIC_VALID'] == 0]))
			self.__valid = self.__valid[self.__valid['BUSINESS_LOGIC_VALID'] == 1]
			self.__valid[column_to_be_checked] = self.__valid[column_to_be_checked].apply(pd.to_numeric, errors='coerce')
			print("valid:", self.__valid.shape[0])
			self.print_invalid()
			print("End of checkPositiveNumeric\n")
			return True

		except Exception as e:
			print("Exception in dfvaliditycheck")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e

	def checkValue(self, values_to_be_checked:dict, column_value_to_be_assigned:dict={}):
		"""
		Check various value level validations

		:param dict values_to_be_checked: list of columns to be checked
		:param dict column_value_to_be_assigned: dictionary to facilitate update in any column other than BUSINESS_LOGIC_VALID

		Categories of value in the dictionary 'value_to_be_checked':
			None: value cannot be None/NULL
			isnumeric: value has to be positive numeric (including decimal value)
			isinteger: value has to be positive integer
			list: value should match one of the elements in the list
			dict: value should match another with another numeric column in the dataframe.
					key of the dictionary is the name of the column to be compared to.
					value of the dictionary is a list whose first element 0th element specifies the threshold value (0 for exact match) and 1st element specifies the number of decimal point for rounding.
			default case: value should exactly match with the column

		"""
		print("\nIn checkValue,\nvalues_to_be_checked: ", values_to_be_checked,
			  " column_value_to_be_assigned: ", column_value_to_be_assigned)
		print("valid:", self.__valid.shape[0])
		self.print_invalid()
		if self.__valid.empty:
			print("no valid data to check")
			return True
		try:
			column_value_to_be_assigned.update({'BUSINESS_LOGIC_VALID': 0})
			numericcolumns = []
			integercolumns = []
			for key, value in values_to_be_checked.items():
				if value == None:
					self.__valid.loc[self.__valid[key].isnull(
					), 'REMARK'] = self.__valid['REMARK']+key+' is invalid'+' , '
					self.__valid.loc[self.__valid[key].isnull(), list(
						column_value_to_be_assigned.keys())] = list(column_value_to_be_assigned.values())
				elif value == 'isnumeric':
					numericcolumns.append(key)
				elif value == 'isinteger':
					integercolumns.append(key)
				elif type(value) is list:
					condition = True
					for x in value:
						condition = condition & (self.__valid[key] != str(x))
					self.__valid.loc[condition,
								   'REMARK'] = self.__valid['REMARK']+key+' is invalid'+' , '
					self.__valid.loc[condition, list(column_value_to_be_assigned.keys())] = list(
						column_value_to_be_assigned.values())
				elif type(value) is dict:
					for key1, value in value.items():
						self.__valid.loc[(pd.to_numeric(self.__valid[key].round(value[1]), errors='coerce') - pd.to_numeric(self.__valid[key1].round(value[1]), errors='coerce')).abs() > value[0], 'REMARK'] = self.__valid['REMARK']+key+' does not match '+key1+' , '
						self.__valid.loc[(pd.to_numeric(self.__valid[key].round(value[1]), errors='coerce') - pd.to_numeric(self.__valid[key1].round(value[1]), errors='coerce')).abs() > value[0], list(
							column_value_to_be_assigned.keys())] = list(column_value_to_be_assigned.values())
				else:
					self.__valid.loc[self.__valid[key] != self.__valid[value], 'REMARK'] = self.__valid['REMARK']+key+' does not match '+value+' , '
					self.__valid.loc[self.__valid[key] != self.__valid[value], list(
						column_value_to_be_assigned.keys())] = list(column_value_to_be_assigned.values())

			self.__invalid.append(
				self.__valid[self.__valid['BUSINESS_LOGIC_VALID'] == 0])
			self.__valid = self.__valid[self.__valid['BUSINESS_LOGIC_VALID'] == 1]
			exec_status = self.checkPositiveNumeric(
				numericcolumns, decimal=True, column_value_to_be_assigned={})
			if exec_status != True:
				return exec_status
			print("valid:", self.__valid.shape[0])
			exec_status = self.checkPositiveNumeric(
				integercolumns, decimal=False, column_value_to_be_assigned={})
			if exec_status != True:
				return exec_status
			print("valid:", self.__valid.shape[0])
			self.print_invalid()
			print("End of checkValue\n")
			return True
		except Exception as e:
			print("Exception in dfvaliditycheck")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e

	def updateValueFromText(self, values_to_be_checked:dict, matchtype:str, column_value_to_be_assigned:dict={}):
		"""
		Update value of certain columns based on another column

		:param dict values_to_be_checked: list of columns to be checked
		:param str matchtype: whether check to be on equal or not equal
		:param dict column_value_to_be_assigned: columns and values to be assigned
		"""
		print("\nIn updateValueFromText,\n", matchtype, "values_to_be_checked: ",
			  values_to_be_checked, " column_value_to_be_assigned: ", column_value_to_be_assigned)
		print("valid:", self.__valid.shape[0])
		self.print_invalid()
		if self.__valid.empty:
			print("no valid data to check")
			return True
		try:
			for key, value in values_to_be_checked.items():
				if matchtype == 'not equal':
					condition = True
					for x in value:
						condition = condition & (self.__valid[key] != str(x))
				elif matchtype == 'equal':
					condition = False
					for x in value:
						print(self.__valid[key], str(x))
						condition = condition | (self.__valid[key] == str(x))
			print(condition)
			for key, value in column_value_to_be_assigned.items():
				self.__valid.loc[condition, key] = value

			print("valid:", self.__valid.shape[0])
			self.print_invalid()
			print("End of updateValueFromText\n")
			return True
		except Exception as e:
			print("Exception in dfvaliditycheck")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e

	def updateValueFromColumn(self, values_to_be_checked:dict, matchtype:str, column_value_to_be_assigned:dict={}, separator:str="/"):
		"""
		Update value of certain columns other columns

		:param dict values_to_be_checked: list of columns to be checked
		:param str matchtype: whether check to be on equal or not equal
		:param dict column_value_to_be_assigned: columns and values to be assigned
		:param str separator: separator to be used in case of concatenating multiple columns
		"""
		print("\nIn updateValueFromColumn,\n", matchtype, "values_to_be_checked: ",
			  values_to_be_checked, " column_value_to_be_assigned: ", column_value_to_be_assigned)
		print("valid:", self.__valid.shape[0])
		self.print_invalid()
		if self.__valid.empty:
			print("no valid data to check")
			return True
		try:
			for key, value in values_to_be_checked.items():
				if matchtype == 'not equal':
					condition = True
					for x in value:
						condition = condition & (self.__valid[key] != str(x))
				elif matchtype == 'equal':
					condition = False
					for x in value:
						condition = condition | (self.__valid[key] == str(x))

			for key, value in column_value_to_be_assigned.items():
				self.__valid.loc[condition, key] = self.__valid[value].apply(
					lambda x: separator.join(x[x.notnull()]), axis=1)

			print("valid:", self.__valid.shape[0])
			self.print_invalid()
			print("End of updateValueFromColumn\n")
			return True
		except Exception as e:
			print("Exception in dfvaliditycheck")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e

	def keepFirst(self, dup_entries_id:pd.DataFrame, criterialist:list, dedupcriterialist:list, identify_duplicate:str=None):
		"""
		Identifying exact duplicate entries and retain the first entry

		:param dataframe dup_entries_id: dataset consisting of all duplicate entries
		:param list criterialist: list of columns to consider for duplicate check
		:param list dedupcriterialist: list of columns to consider for identifying exact duplicate entries
		:param str identify_duplicate: column to use to identify groups of duplicate entries and add remark accordingly
		"""
		print("In keepFirst,\ncriterialist: ", criterialist,
			  " dedupcriterialist: ", dedupcriterialist)
		print("valid:", self.__valid.shape[0])
		self.print_invalid()
		# print("dup_entries_id headers: ", dup_entries_id.columns)
		try:
			grouped = list(dup_entries_id.groupby(criterialist))
			# print(len(grouped))
			temp_valid = pd.DataFrame()
			temp_invalid = pd.DataFrame()
			for i in range(len(grouped)):
				temp_group = grouped[i][1]
				temp_group.index = np.arange(len(temp_group))
				dedup_combo = temp_group[dedupcriterialist].drop_duplicates()
				if not dedup_combo.empty:
					temp_invalid = pd.concat(
						[temp_invalid, temp_group], ignore_index=True)
				else:
					temp_group.loc[temp_group.index == 0, [
						'BUSINESS_LOGIC_VALID', 'REMARK']] = [1, '']
					temp_invalid = pd.concat(
						[temp_invalid, temp_group[temp_group['BUSINESS_LOGIC_VALID'] == 0]], ignore_index=True)
					temp_valid = pd.concat(
						[temp_valid, temp_group[temp_group['BUSINESS_LOGIC_VALID'] == 1]], ignore_index=True)

			if identify_duplicate is None:
				self.__invalid.append(temp_invalid)
			else:
				exec_status = self.identifyDuplicate(
					temp_invalid, identify_duplicate, criterialist)
				if exec_status != True:
					return exec_status
			self.__valid = pd.concat([self.__valid, temp_valid], ignore_index=True)
			# exec_status = self.removeColumns(['DUP_FINAL', 'KEEP_FIRST'])
			# if exec_status != True:
			#     return exec_status
			print("valid:", self.__valid.shape[0])
			self.print_invalid()
			print("End of keepFirst\n")
			return True

		except Exception as e:
			print("Exception in dfvaliditycheck")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e

	def addColumns(self, nameAndValues):
		"""Add columns to dataframe and assign specific value
		
		:param dict nameAndValues: name and value of column to be assigned
		"""
		try:
			for key, value in nameAndValues.items():
				self.__valid[key] = value
			return True
		except Exception as e:
			print("Exception in dfvaliditycheck")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e

	def removeColumns(self, columnName):
		"Remove list of columns from dataframe"
		try:
			self.__valid = self.__valid.drop(columnName, axis=1, errors='ignore')
			return True
		except Exception as e:

			print("Exception in dfvaliditycheck")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e
		
	def checkDependentValues(self,dependentList):
		"""
		Check availability of data in columns that can be null only together

		:param list dependentList: list of group of columns to be checked for null or value
		"""
		try:
			for list in dependentList:

				bool1 = True

				bool2 = True

				for column in list:

					bool1 = bool1 & (self.__valid[column].isna())

					bool2 = bool2 & (self.__valid[column].notna())

				fin_bool = ~(bool1 | bool2)
				self.__valid['BUSINESS_LOGIC_VALID']=True
				self.__valid.loc[fin_bool, 'REMARK'] = 'Either all or none among'+','.join(list)+"must have some value"
				self.__valid.loc[fin_bool, 'BUSINESS_LOGIC_VALID'] = False
				self.__invalid.append(self.__valid[self.__valid['BUSINESS_LOGIC_VALID']==False])
				self.__valid=self.__valid[self.__valid['BUSINESS_LOGIC_VALID']==True]
				return True
		except Exception as e:
			print("Exception in checkDependentValues")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e	
	
	def addColumnFromFormula(self, formulaList:str,dfName:str="valid"):
		"""
		Adds columns based on the formula given in the "formulaList" from dataframe
		e.g. of formulaList:  
		'''
		new column_1=old_column_1+old_column_2+100
		new column_2=old_column_1+old_column_2+500
		'''
		"dfName" can be "valid" or "invalid", default is "valid"
		"""
		try:
			
			if dfName=="valid":
				if not self.__valid.empty:
					self.__valid=self.__valid.eval(formulaList)
			elif dfName=="invalid":
				for x in range(self.__invalid.length):
					if not self.__invalid[x].empty:
						self.__invalid[x]=self.__invalid[x].eval(formulaList)
			else:
				raise Exception ("Invalid dfName")
			return True
		except Exception as e:
			print("Exception in dfvaliditycheck")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			print(e, exc_type, fname, exc_tb.tb_lineno)
			print("Exception: ", e)
			return e