from flask import Flask, request, jsonify
import Main






app = Flask(__name__)
app.secret_key = 'HZt,}`v{&pwv&,qvtSV8Z9NE!z,p=e?('


@app.route('/')
def Index():
    return 'Area Big Data Server'

# 빅데이터 분석 요청
@app.route('/big_data', methods=['GET'])
def big_data():
    place_id = request.args.get('place_id')
    score = Main.load_bigdata(place_id)
    return jsonify({"result": score})






if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=14565)
