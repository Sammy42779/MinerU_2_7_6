FROM hub.yun.paic.com.cn/icore-aas-python/chatlm:wiseapm_py3.8_cuda11.3_torch2.0.1_transformers4.31.0_accelerate0.23.0_bitsandbytes0.41.1_peft0.5.0
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV PYTHONIOENCODING utf8



COPY ./ /app
WORKDIR /app

RUN pip3 install --upgrade pip -i http://mirrors.yun.paic.com.cn:4048/pypi_new/web/simple --trusted-host mirrors.yun.paic.com.cn
RUN pip3 uninstall -y pycrypto
RUN pip3 install -r requirements.txt -i http://maven.paic.com.cn/repository/pypi/simple/ --trusted-host maven.paic.com.cn

RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo 'Asia/Shanghai' >/etc/timezone

ENTRYPOINT sed -i -e "/app_name =/ s/=.*/=$APP_NAME/" \
  -e "/agent_id =/ s/=.*/=$HOSTNAME/" \
  -e "/collector_ip =/ s/=.*/=$COLLECTOR_IP/" \
  && NEW_RELIC_CONFIG_FILE=/wls/wls81/detector-python-agent/newrelic.ini python3 /wls/wls81/detector-python-agent/newrelic-admin run-program \
  python3 /app/mineru_flask_main.py