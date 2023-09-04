SHELL=/bin/bash
devops_state = main
working_dir = `pwd`

install: local_build_and_deploy

reinstall : create_env && install

local_build_and_deploy: 
	pip uninstall snowpark_session -y \
	&& python setup.py install \
	&& snowpark_session

package_build:
	python -m build

package_list:
	unzip -l dist/*.whl  

create_env:
	conda deactivate -n snowpark_session \
	&& conda env remove -n snowpark_session -y \
	&& conda create -n snowpark_session python=3.8 -y \
	&& conda activate snowpark_session
