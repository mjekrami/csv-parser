FROM python3.10
copy src/ /app
WORKDIR /app
RUN pip3 install -r requirements.txt
CMD ["python3","main.py"]