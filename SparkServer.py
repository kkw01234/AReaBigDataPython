from flask import Flask, request, jsonify
import Main
import pymysql
from Propensity import propensity_restaurant
import traceback

app = Flask(__name__)
app.secret_key = 'HZt,}`v{&pwv&,qvtSV8Z9NE!z,p=e?('
private_key = '95apduobleerstcy97'

@app.route('/')
def Index():
    return 'Area Big Data Server'

# 빅데이터 분석 요청
@app.route('/big_data', methods=['GET'])
def big_data():
    place_id = request.args.get('place_id')
    key = request.args.get('key')
    if key == private_key:
        try:
            score = Main.load_bigdata(place_id)
        except:
            import traceback
            traceback.print_exc()
            return jsonify({"result": "Error"})
    else:
        return jsonify({"result": "Error"})
    # 서버에 저장해주는 코드
    conn = pymysql.connect(host='118.220.3.71', user='root', password='rjsdnrkkw4809!!', db='area', charset='utf8')
    curs = conn.cursor()
    sql = "UPDATE restaurant SET restpoint ='" + str(score * 100) + "' WHERE restgoogleid='" + place_id + "'"
    curs.execute(sql)
    conn.commit()
    return jsonify({"result": score})


#딥러닝 학습 시키는 코드
@app.route('/deep_learning', methods=['GET'])
def deep_learnig():
    key = request.args.get('key')
    if key == private_key:
        import ReviewAnalysis
        ReviewAnalysis.deep_learning()


@app.route('/similar', methods=['GET'])
def similar_restaurant():
    try:
        key = request.args.get('key')  # ['key']
        if key != private_key:
            return jsonify({"result": "Error"})
    except:
        return jsonify({"result": "Error"})
    try:
        u_id = request.args.get('user_id')  # .form['user_id']
        rest_google_id = request.args.get('google_id')  # .form['google_id']
        result = propensity_restaurant(u_id, rest_google_id)
        print(round((result+1)*50, 0), " ", result) # 범위가 -1~1
        return jsonify({'result': str(round((result+1)*49-1, 0))})
    except:
        traceback.print_exc()
        return jsonify({'result': 'Error'})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=14565)
