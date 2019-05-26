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
    key = request.args.get('key')
    if key == '123456':
        score = Main.load_bigdata(place_id)
    else:
        return jsonify({"result": "Key Error"})
    return jsonify({"result": score})


''' #딥러닝 학습 시키는 코드
@app.route('/deep_learning', methods=['GET'])
def deep_learnig():
    key = request.args.get('key')
    if key == '123456':
        import ReviewAnalysis
'''


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=14565)
