# 使用python镜像自定义我们的镜像
FROM python:3.8
#创建/app目录
RUN mkdir /app

#将项目文件内的requirements.txt拷贝到/app
COPY requirements.txt /app
#COPY ./setup.py /app
COPY app /app

#切换到/app目录
WORKDIR /app
#使用豆瓣源https://pypi.douban.com/simple安装项目的python依赖
#RUN python3 -m pip install --upgrade pip
RUN pip3 install -r requirements.txt --trusted-host https://pypi.douban.com/simple -i https://pypi.douban.com/simple

#RUN pip3 install -r requirements.txt --trusted-host https://pypi.tuna.tsinghua.edu.cn/simple -i https://pypi.tuna.tsinghua.edu.cn/simple

#RUN pip3 install -r requirements.txt

#使用Cython对代码进行混淆
#RUN python setup.py build_ext --inplace

#删除源代码
#RUN rm -rf api
#RUN rm -rf utils
#RUN rm -rf logs
#RUN rm *.py

#将内部的8000端口暴露出去
#EXPOSE 8019

ENV PYTHONPATH "${PYTHONPATH}:/app"

#ARG FILE_NAME

#使用uvicorn启动应用，IP为0.0.0.0，端口为8000（这里的端口是8000，那么EXPOSE暴露的端口也必须是这个）
CMD ["python3", "ib_account.py"]

