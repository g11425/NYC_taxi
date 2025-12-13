import logging
import os
import os.path
import shutil

class FileOps:
	

	def __init__(self, project_path):
		self.project_path = project_path
		self.data_path = os.path.join(project_path, "data")
		self.data_path_raw = os.path.join(self.data_path, "raw")
		self.data_path_processed = os.path.join(self.data_path, "processed")
		self.test_file_raw = "test.csv"
		self.test_files_processed = ["test1.csv","test2.csv","test3.csv"]

	def setup_data_paths(self):
		
		try:

			if not (os.path.exists(self.data_path)):
				os.mkdir(self.data_path)

			if not (os.path.exists(self.data_path_raw)):
				os.mkdir(self.data_path_raw)

			if not (os.path.exists(self.data_path_processed)):
				os.mkdir(self.data_path_processed)
			return True

		except Exception as e:
			logging.exception(e)
			return False


	def clear_data_paths(self):
		try:
			if os.path.exists(self.data_path_raw):
				shutil.rmtree(self.data_path_raw)
			if os.path.exists(self.data_path_processed):
				shutil.rmtree(self.data_path_processed)
			return True

		except Exception as e:
			logging.exception(e)
			return False
	
	def create_dummy_raw(self):
		self.setup_data_paths()
		try:
			file=open(self.get_test_raw_file_name_abs(), mode="w")
			file.write("col1,col2,col3")
			file.close()
			return True
		except Exception as e:
			logging.exception(e)
			return False


	def clean_dummy_raw(self):
		try:
			if os.path.exists(self.get_test_raw_file_name_abs()):
				os.remove(self.get_test_raw_file_name_abs())
				return True
		except Exception as e:
			logging.exception(e)
			return False


	def check_raw_path(self):
		return os.path.exists(self.data_path_raw)

	def get_test_raw_file_name_abs(self):
		return os.path.join(self.data_path_raw, self.test_file_raw)

	def create_dummy_processed(self):
		self.setup_data_paths()
		try:
			for file in self.test_files_processed:
				file_abs = os.path.join(self.data_path_processed, file)
				f = open(file_abs, "w")
				f.write("ID,Val")
				f.close()
		except Exception as e:
			logging.exception(e)

	def clean_dummy_processed(self):
		try:
			for file in self.test_files_processed:
				file_abs = os.path.join(self.data_path_processed, file)
				if os.path.exists(file_abs):
					os.remove(file_abs)
		except Exception as e:
			logging.exception(e)

	def get_test_processed_file_names_abs(self):
		out = []
		for file in self.test_files_processed:
			file_abs = os.path.join(self.data_path_processed, file)
			out.append(file_abs)
		return out
