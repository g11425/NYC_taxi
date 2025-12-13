import logging
import os
import shutil


class FileOps:
	
	data_path_raw = "/data/raw/"
	data_path_processed = "/data/processed/"

	def __init__(self, project_path):
		self.project_path = project_path
		self.data_path = os.path.join(project_path, "data")
		self.data_path_raw = os.path.join(self.data_path, "raw")
		self.data_path_processed = os.path.join(self.data_path, "processed")


	def check_for_paths(self):
		
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
	
