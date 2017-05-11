from flask import Flask
import prepare_html_files
import codecs

app = Flask(__name__)

@app.route('/')
def main():
    prepare_html_files.main
    f=codecs.open("showcase.html", 'r')
    return f.read()

# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    # application.debug = True
    app.run("0.0.0.0", port=80)





# if __name__ == '__main__':
#     main()
