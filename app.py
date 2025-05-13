import os
import pandas as pd
from flask import Flask, request, render_template, send_file, redirect, url_for, session
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'your_secret_key'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_FOLDER'] = 'processed'

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'csv', 'tsv', 'xlsx'}


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        hpo_ids = request.form['hpo_ids'].split(';')
        uploaded_file = request.files['file']

        if uploaded_file and allowed_file(uploaded_file.filename):
            filename = secure_filename(uploaded_file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            uploaded_file.save(filepath)

            session['hpo_ids'] = [h.strip() for h in hpo_ids if h.strip()]
            session['filepath'] = filepath

            # Load file and get columns
            if filename.endswith('.csv'):
                df = pd.read_csv(filepath)
            elif filename.endswith('.tsv'):
                df = pd.read_csv(filepath, sep='\t')
            else:
                df = pd.read_excel(filepath)

            session['columns'] = df.columns.tolist()
            df.to_pickle("uploads/raw.pkl")  # Save raw data
            return redirect(url_for('select_column'))

    return render_template('index.html')


@app.route('/select_column', methods=['GET', 'POST'])
def select_column():
    columns = session.get('columns', [])
    if request.method == 'POST':
        selected_column = request.form['column']
        hpo_ids = session['hpo_ids']
        filepath = session['filepath']

        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        elif filepath.endswith('.tsv'):
            df = pd.read_csv(filepath, sep='\t')
        else:
            df = pd.read_excel(filepath)

        matches = []
        for idx, row in df.iterrows():
            cell = str(row[selected_column])
            terms = [t.strip() for t in cell.split(',')]
            matched = list(set(hpo_ids).intersection(terms))
            if len(matched) >= 3:
                row_data = row.to_dict()
                row_data['Matched HPO Count'] = len(matched)
                row_data['Matched HPOs'] = ', '.join(matched)
                matches.append(row_data)

        result_df = pd.DataFrame(matches)
        result_df.sort_values(by='Matched HPO Count', ascending=False, inplace=True)

        outpath = os.path.join(app.config['PROCESSED_FOLDER'], 'matched.xlsx')
        result_df.to_excel(outpath, index=False)
        session['filtered_file'] = outpath
        session['filterable_columns'] = result_df.columns.tolist()
        return render_template('results.html', tables=[result_df.to_html(classes='data')])
    return render_template('select_column.html', columns=columns)


@app.route('/download')
def download_file():
    return send_file(session.get('filtered_file'), as_attachment=True)


@app.route('/filter', methods=['GET', 'POST'])
def filter():
    columns = session.get('filterable_columns', [])
    if request.method == 'POST':
        selected_cols = request.form.getlist('columns')
        session['selected_cols'] = selected_cols
        return redirect(url_for('filter_values', index=0))
    return render_template('filter_select.html', columns=columns)


@app.route('/filter_values/<int:index>', methods=['GET', 'POST'])
def filter_values(index):
    df = pd.read_excel(session['filtered_file'])
    selected_cols = session.get('selected_cols', [])

    if index >= len(selected_cols):
        return redirect(url_for('download_file'))

    col = selected_cols[index]
    values = sorted(df[col].dropna().unique())
    is_numeric = pd.api.types.is_numeric_dtype(df[col])

    if request.method == 'POST':
        if is_numeric:
            op = request.form['op']
            val = float(request.form['value'])
            if op == 'lt':
                df = df[df[col] < val]
            elif op == 'gt':
                df = df[df[col] > val]
            elif op == 'lte':
                df = df[df[col] <= val]
            elif op == 'gte':
                df = df[df[col] >= val]
        else:
            selected = request.form.getlist('values')
            df = df[df[col].isin(selected)]

        path = os.path.join(app.config['PROCESSED_FOLDER'], f'filtered_{index}.xlsx')
        df.to_excel(path, index=False)
        session['filtered_file'] = path
        return redirect(url_for('filter_values', index=index + 1))

    return render_template('filter_values.html', column=col, values=values, is_numeric=is_numeric)


if __name__ == '__main__':
    app.run(debug=True)
