from flask import Flask, jsonify
from spark_queries import Queries

q = Queries()

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False


@app.route('/')
def sparkRestAPI():
    return 'This is Spark Rest API'

# Query - 1
@app.route('/max_diff_stock_percent')
def max_diff_stock_percent():
    data = q.max_diff_stock_percent()
    return jsonify(data)

# Query - 2
@app.route('/most_traded_stock_each_day')
def most_traded_stock_per_day():
    data = q.most_traded_stock_per_day()
    return jsonify(data)

# Query - 3
@app.route('/max_gap')
def stock_with_most_gap():
    data = q.stock_with_most_gap()
    return jsonify(data)


# Query 4
@app.route('/api/stock_with_max_movement')
def api_max_movement_stock():
    data = q.stock_with_max_movement()
    return jsonify(data)


# query 5
@app.route('/api/standard_deviation')
def api_standard_deviation():
    data = q.standard_deviation_for_each_stock()
    return jsonify(data)

# Query - 6
@app.route('/mean_median_stocks')
def mean_median_for_each_stock():
    data = q.mean_median_for_each_stock()
    return jsonify(data)


# query 7
@app.route('/api/average_volume')
def api_average_volume():
    data = q.average_volume_per_stock()
    return jsonify(data)


# query 8
@app.route('/api/higher_average_volume')
def api_higher_average_volume():
    data = q.stock_with_highest_average_volume()
    return jsonify(data)


# query 9
@app.route('/api/highest_lowest_stock_price')
def api_highest_lowest_stock_price():
    data = q.highest_lowest_price_for_each_stock()
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)