import './polyfill';

export const gRows = Symbol('gRows');

interface Row {
    [k: string]: any
}
interface Values {
    [k: string]: any[]
}
interface AggFn {
    (df: DataFrame): [string, any]
}
interface RowFn {
    (df: DataFrame): any
}
interface Group {
    [gRows]: DataFrame;
    [k: string]: any;
}
interface NameMapping {
    [k: string]: string
}
type Order = 'asc' | 'desc';

class Col {
    expr: string;
    alias?: string;

    constructor(expr: string = '') {
        this.expr = expr;
    }

    as(alias: string) {
        this.alias = alias;
        return this;
    }
}

function toCol(column: string | Col): Col {
    if(column instanceof Col) {
        return column
    } else {
        return new Col(column);
    }
}

export function col(expr?: string) {
    return new Col(expr);
}

// [ Row, Row, Row ]
export class DataFrame {
    rows: Row[];
    values: Values;

    constructor(rows: Row[]) {
        this.rows = [...rows];
        this.values = {};
    }

    get columns(): string[] {
        return this.rows.length ? Object.keys(this.rows[0]) : [];
    }

    top(num: number) {
        this.values = {};
        this.rows = this.rows.slice(0, num);
        return this;
    }

    filter(expr: string) {
        const exprFn = genExprFn(expr, this.columns);
        this.rows = this.rows.filter(row => exprFn.apply(null, this.getRowValues(row)));
        return this;
    }

    groupBys(columns: Array<string | Col>): DataFrame | GroupDataFrame {
        const cols = columns.map(column => toCol(column));
        return cols.reduce((ret : DataFrame | GroupDataFrame, col) => ret.groupBy(col), this);
    }

    groupBy(column: string | Col): GroupDataFrame {
        const col = toCol(column);
        const exprFn = genExprFn(col.expr, this.columns);
        // { group1: [row, row, row],  group2: [row, row, row]}
        const g = this.rows.reduce((g: Values, row) => {
            const groupName = exprFn.apply(null, this.getRowValues(row))
            if(g[groupName]) {
                g[groupName].push(row);
            } else {
                g[groupName] = [row];
            }
            return g;
        }, {});
        // [ { expr: group1, [gRows]: df([row, row, row]) } 
        //   { expr: group2, [gRows]: df([row, row, row]) } ]
        const g2 = Object.entries(g).map(([colValue, rows])=>
            ({ [col.alias || col.expr]: exprFn.apply(null, this.getRowValues(rows[0])), [gRows]: new DataFrame(rows) })
        );

        return new GroupDataFrame(this, g2);
    }

    mapEnum(column: string | Col, options: { [key: string]: string }, defaultValue?: any) {
        const col = toCol(column);
        this.rows.forEach(row => {
            row[col.alias || col.expr] = (options[row[col.expr]] !== undefined) ? options[row[col.expr]] : defaultValue;
        });
        return this;
    }

    mapBool(column: string | Col, trueValue: any, falseValue: any) {
        const col = toCol(column);
        this.rows.forEach(row => {
            row[col.alias || col.expr] = row[col.expr] ? trueValue : falseValue;
        });
        return this;
    }

    select(...colsOrColumns: Array<Col | string>): DataFrame {
        const cols = colsOrColumns.map((colOrColumn: Col | string) => toCol(colOrColumn));
        const fns = cols.map(col => genExprFn(col.expr, this.columns));

        const rows = this.rows.map(row => {
            return fns.reduce((ret, fn, index) => {
                ret[cols[index].alias || cols[index].expr] = fn.apply(null, this.getRowValues(row));
                return ret;
            }, {})
        });

        return new DataFrame(rows);
    }

    agg(...fns: AggFn[]) {
        return new GroupDataFrame(this, [{[gRows]: this}])
            .agg(...fns);
    }

    // the order may need to reconsider.
    getValues(column: string) {
        if (!this.values[column]) {
            const set = new Set();
            this.rows.forEach(row => {
                if(row[column] != null) {
                    set.add(row[column])
                }
            });
            this.values[column] = Array.from(set);
        }
        return this.values[column];
    }

    getRowValues(row: Row) {
        return this.columns.map(colName => row[colName]);
    }

    // make sure data completion before rename
    rename(sMap: { [key: string]: string }, dMap?: (key: string) => string) {
        this.rows = this.rows.map((row: Row) => {
            const renamed: Row =  {};
            this.columns.forEach((key: string) => {
                if (sMap[key]) {
                    renamed[sMap[key]] = row[key];
                } else if (dMap) {
                    renamed[dMap(key)] = row[key];
                } else {
                    renamed[key] = row[key];
                }
            });
            return renamed;
        });
        return this;
    }

    getColumns(...except: string[]) {
        return this.columns.filter(column => !except.includes(column));
    }

    // similar to rowAgg and select expr, but more convinence when add cumputed property
    withColumn(columnName: string, expr: ((row: Row) => any) | string) {
        if(typeof expr === 'string') {
            const exprFn = genExprFn(expr, this.columns);
            this.rows.forEach(row => row[columnName] = exprFn.apply(null, this.getRowValues(row)));
        } else {
            this.rows.forEach(row => row[columnName] = expr(row));
        }
        return this;
    }

    rowAgg(...fns: RowFn[]) {
        fns.forEach(fn => fn(this));
        return this;
    }

    columnAgg(nameCol: string, name: string, fns: AggFn[]) {
        const summary = fns.reduce((ret: Row, fn) => {
            const [column, value] = fn(this);
            ret[column] = value;
            return ret;
        }, {});
        this.rows.push({ [nameCol]: name, ...summary });
        return this;
    }

    orderBy(cols: string | string[], orders: Order | Order[] = ['desc']): DataFrame {
        // make cols and orders same length array
        cols = Array.isArray(cols) ? cols : [cols];
        orders = Array.isArray(orders) ? orders : [orders];
        while (orders.length < cols.length) {
            orders.push('desc');
        }
        if(cols.length === 0) return this;
        this.rows.sort((a, b) => {
            let i = 0;
            while (i < cols.length) {
                const col = cols[i];
                const order = (orders[i] === 'asc' ? 1 : -1);
                if (a[col] > b[col]) {
                    return order
                } else if (a[col] < b[col]) {
                    return -order
                } else if (i === cols.length - 1) {
                    return 0;
                } else {
                    i++
                }
            }
            throw Error('orderBy Method is wrong');
            return 0;
        });
        return this;
    }
}




// [
//   { pId: 'P1', rows: DataFrame }
//   { pId: 'P2', rows: DataFrame }
// ]
export class GroupDataFrame {
    df: DataFrame;
    gData: Group[];

    constructor(dataframe: DataFrame, gData: Group[]) {
        this.df = dataframe;
        this.gData = gData;
    }

    // 因为不知道怎么处理嵌套groupby, 所以我把group的列拍平了.
    // return GroupDataFrame
    // [
    //   { pId: 'P1', country: 'US', rows: [Row] }
    //   { pId: 'P1', country: 'UK', rows: [Row, Row] }
    // ]
    groupBy(column: string | Col) {
        const col = toCol(column);
        const gData = this.gData.flatMap(gRow => {
            const { [gRows]: dataFrame, ...rest } = gRow;
            return dataFrame.groupBy(col.expr).gData.map(gRow => ({ ...rest, ...gRow }));
        });
        return new GroupDataFrame(this.df, gData);
    }
    
    // pivot
    // 将columnToPivot这一列数据，变成新的列，数据聚合内容
    pivot(columnToPivot: string, aggFns: AggFn): DataFrame;
    pivot(columnToPivot: string, aggFns: AggFn[]): DataFrame[];
    pivot(columnToPivot: string, aggFns: AggFn | AggFn[]) {
        // 提前遍历获得所有新列名，保证行数据的完整性 test: groupDataframe pivot with uncompelete data
        // 提前遍历获得所有新列名，保证行数据的完整性 test: groupDataframe pivot with uncompelete data
        const newColumnNames = this.df.getValues(columnToPivot);

        if (!Array.isArray(aggFns)) {
            aggFns = [aggFns];
        }

        const datas = aggFns.map(aggFn => this._pivotAgg(columnToPivot, newColumnNames, aggFn));
        
        if (datas.length > 1) {
            return datas.map(data => new DataFrame(data));
        } else {
            return new DataFrame(datas[0]);
        }
    }

    // [{ date: '01', [gRows]: df } { date: '02', [gRows]: df }]
    // [{ date: '01', foo: 1, bar: 2 } { date: '02', foo: 3, bar: 4 }]
    private _pivotAgg(columnToPivot: string, newColumnNames: string[], aggFn: AggFn) {
        return this.gData.map(gRow => {
            const { [gRows]: df, ...groups } = gRow;
            // { name: 'foo',  [gRows]: } { name: 'bar',  [gRows]: }
            // { foo: aggFn(foo's rows), bar: aggFn(bar's rows)}
            const gdf = df.groupBy(columnToPivot);

            const newRowInit = newColumnNames.reduce((ret: Row, name) => {
                ret[name] = 0;
                return ret;
            }, {});

            const newRow = gdf.gData.reduce((ret: Row, { [gRows]: df, [columnToPivot]: newColumnName }) => {
                ret[newColumnName] = aggFn(df)[1];
                return ret;
            }, newRowInit);

            return {...newRow, ...groups };
        });
    }

    agg(...fns: AggFn[]) {
        const data = this.gData.map(gRow => {
            const { [gRows]: dataFrame, ...rest } = gRow;
            const ret = fns.reduce((ret: Row, fn) => {
                const [key, value] = fn(dataFrame);
                ret[key] = value;
                return ret;
            }, {});
            return { ...rest, ...ret };
        });
        return new DataFrame(data);
    }
}

export const aggFn = {
    sum: (columnOrCol: string | Col) => (df: DataFrame): [string, number] => {
        const col = toCol(columnOrCol);
        const { rows } = df;
        const value = rows.reduce((prev, row) => prev + (row[col.expr] || 0), 0);
        return [col.alias || col.expr, value];
    },
    count: (columnOrCol: string | Col) => (df: DataFrame): [string, number] => {
        const col = toCol(columnOrCol);
        const value = df.rows.length;
        return [col.alias || col.expr, value];
    }
};

export const rowFn = {
    sum: (expect: string[], col?: Col) => (df: DataFrame) => {
        const { rows } = df;
        const key = (col && col.alias) || 'sum';
        const columns = df.getColumns(...expect);
        rows.forEach(row => {
            row[key] = columns.reduce((sum, col) => {
                return sum + row[col] || 0;
            }, 0);
        })
    },
    percent: (columnOrCol: string | Col, withPercentSign = true) => (df: DataFrame) => {
        const col = toCol(columnOrCol);
        const key = col.alias || 'percent';
        const total = aggFn.sum(col.expr)(df)[1];
        df.rows.forEach(row => {
            const percent = total ? (row[col.expr] / total) : 0;
            row[key] = withPercentSign ? (percent * 100).toFixed(2) + '%' : Number((percent * 100).toFixed(2));
        });
    }
};

interface Token {
    type: string,
    value: any
}

function lexer(expr: string): Token[] {
    const tokens = [];
    for (let word of expr.split(/\s+/)) {
        if(word.match(/^[a-zA-Z][\w]+$/) /* [a-zA-Z0-9_] */) {
            tokens.push({ type: 'identifier', value: word })
        } else if(word.match(/[\+\-\*\/]/) /* + * - / */) {
            tokens.push({ type: 'operator', value: word})
        } else if(word.match(/\d+/) /* only support integer TODO support double*/) {
            tokens.push({ type: 'literal', value: word})
        }
    }
    return tokens;
}

function parser(tokens: Token[]) {
    let str = "return ";
    for (let {type, value} of tokens) {
        switch (type) {
            case 'identifier': {
                str += `row['${value}'] `;
                break;
            }
            default: {
                str += `${value} `
            }
        }
    }
    return new Function('row', str);
}

export function genExprFn(expr: string, keys: string[]) {
    return new Function(`return function (${keys.join(',')}) { return ${expr}}`)();
}

// legend 默认除了category的所有列
//        string[] 选择这几列作为legend
//        { [key: string]: string } 选择key这几列, 并且改名
export function toLineChart(df: DataFrame, categoryKey: string, legendOpt?: string[] | { [key: string]: string }) {
    let legends, columns, isObject = false;
    if (!legendOpt) {
        columns = legends = df.getColumns(categoryKey);
    } else if (Array.isArray(legendOpt)) {
        columns = legends = legendOpt;
    } else {
        columns = Object.keys(legendOpt);
        legends = columns.map(col => legendOpt[col]);
        isObject = true;
    }
    return {
        legends,
        categories: df.rows.map(r => r[categoryKey]),
        series: columns.reduce((ret: any, col) => {
            ret[isObject ? (legendOpt as any)[col]: col] = df.rows.map(r => r[col])
            return ret;
        }, {})
    }
}

// data [{ name: xx, value: 123}, { name: xx, value: 123}]
export function toPieChart(df: DataFrame, nameCol: string = 'name', valueCol: string = 'value') {
    return {
        legends: df.getValues(nameCol),
        data: df.rows.map(row => ({ name: row[nameCol], value: row[valueCol] })),
    };
}
