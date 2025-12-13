import logging
import os


class FileOps:
	
	project_path = ""
	data_path_raw = "/data/raw/"
	data_path_processed = "/data/processed/"

	def __init__(self, project_path):
		self.project_path = project_path
		self.check_for_paths()


	def check_for_paths(self):
		
		self.raw_path = self.project_path + self.data_path_raw
		self.processed_path = self.project_path + self.data_path_processed

		try:

			if not (os.path.exists(self.project_path + "/data")):
				os.mkdir(self.project_path + "/data")

			if not (os.path.exists(raw_path)):
				os.mkdir(raw_path)

			if not (os.path.exists(processed_path)):
				os.mkdir(processed_path)
			return True

		except Exception as e:
			logging.exception(e)
			return False


	def clear_data_paths(self):
		try:
			if os.path.exists(self.raw_path):
				os.rmdir(self.raw_path)
			if os.path.exists(self.processed_path):
			    os.rmdir(self.processed_path)
			return True
		except Exception as e:
			logging.exception(e)
			return False
	
