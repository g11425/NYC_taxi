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
			if os.path.exists(self.test_file_raw):
				os.remove(self.test_file_raw)
				return True
		except Exception as e:
			logging.exception(e)
			return False


	def check_raw_path(self):
		return os.path.exists(self.data_path_raw)

	def get_test_raw_file_name_abs(self):
		return os.path.join(self.data_path_raw, self.test_file_raw)