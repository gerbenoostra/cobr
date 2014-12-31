import os
import errno


def make_sure_path_exists(path):
	"""
	Creates directories up to including path, does not fail if it already exist

	Does not fails if there is already a file at the pointed to path
	:param path: the path (can be multiple folders deep) which should exist.
	:return: nothing is returned. An OSError is raised in case
	"""
	try:
		os.makedirs(path)
	except OSError as exception:
		if exception.errno != errno.EEXIST:
			raise