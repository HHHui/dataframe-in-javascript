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
interface Group {
    [gRows]: DataFrame;
    [k: string]: any;
}
interface NameMapping {
    [k: string]: string
}

// [ Row, Row, Row ]
export class DataFrame {
    rows: Row[];
    values: Values;
    columns: string[]; // columns为row有哪些列。

    constructor(rows: Row[]) {
        this.rows = rows;
        this.values = {};
        this.columns = rows.length ? Object.keys(rows[0]) : [];
    }

    groupBy(expr: string) {
        const exprFn = genExprFn(expr, this.columns);
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
            ({ [expr]: exprFn.apply(null, this.getRowValues(rows[0])), [gRows]: new DataFrame(rows) })
        );

        return new GroupDataFrame(this, g2);
    }

    select(expr: string) {
        const fn = parser(lexer(expr));

        const rows = this.rows.map(row => {
            return { [expr]: fn(row) }
        });

        return new DataFrame(rows);
    }

    agg(fn: AggFn) {
        return fn(this)[1];
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
    groupBy(column: string) {
        const gData = this.gData.flatMap(gRow => {
            const { [gRows]: dataFrame, ...rest } = gRow;
            return dataFrame.groupBy(column).gData.map(gRow => ({ ...rest, ...gRow }));
        });
        return new GroupDataFrame(this.df, gData);
    }
    
    // pivot
    // 将columnToPivot这一列数据，变成新的列，数据聚合内容
    pivot(nameOrCol: string | Col, aggFns: AggFn): DataFrame;
    pivot(nameOrCol: string | Col, aggFns: AggFn[]): DataFrame[];
    pivot(nameOrCol: string | Col, aggFns: AggFn | AggFn[]) {
        // 提前遍历获得所有新列名，保证行数据的完整性 test: groupDataframe pivot with uncompelete data
        const col = toCol(nameOrCol);
        const newColumnNames = this.df.getValues(col.name);
        // if col as exists
        const newColumnNameMappings = col.renameTemplate ? 
            newColumnNames.reduce((prev, cv) => {
                prev[cv] = col.transform(cv);
                return prev;
            }, {}) : null;

        if (!Array.isArray(aggFns)) {
            aggFns = [aggFns];
        }

        const datas = aggFns.map(aggFn => this._pivotAgg(col.name, newColumnNames, aggFn, newColumnNameMappings));
        
        if (datas.length > 1) {
            return datas.map(data => new DataFrame(data));
        } else {
            return new DataFrame(datas[0]);
        }
    }

    // [{ date: '01', [gRows]: df } { date: '02', [gRows]: df }]
    // [{ date: '01', foo: 1, bar: 2 } { date: '02', foo: 3, bar: 4 }]
    _pivotAgg(columnToPivot: string, newColumnNames: string[], aggFn: AggFn, newColumnNameMappings: NameMapping) {
        return this.gData.map(gRow => {
            const { [gRows]: df, ...groups } = gRow;
            // { name: 'foo',  [gRows]: } { name: 'bar',  [gRows]: }
            // { foo: aggFn(foo's rows), bar: aggFn(bar's rows)}
            const gdf = df.groupBy(columnToPivot);

            const newRowInit = newColumnNames.reduce((ret: Row, name) => {
                ret[name] = 0;
                return ret;
            }, {});

            let newRow = gdf.gData.reduce((ret: Row, { [gRows]: df, [columnToPivot]: newColumnName }) => {
                ret[newColumnName] = df.agg(aggFn);
                return ret;
            }, newRowInit);

            // if col as exists
            if(newColumnNameMappings) {
                newRow = newColumnNames.reduce((ret: Row, colName) => {
                    ret[newColumnNameMappings[colName]] = newRow[colName];
                    return ret;
                }, {});
            }

            return {...newRow, ...groups };
        });
    }

    agg(fn: AggFn) {
        const data = this.gData.map(gRow => {
            const { [gRows]: dataFrame, ...rest } = gRow;
            const [key, value] = fn(dataFrame);
            return { ...rest, [key]: value };
        });
        return new DataFrame(data);
    }
}

export class Col {
    name: string;
    renameTemplate?: string;

    constructor(name: string) {
        this.name = name;
    }
    as(renameTemplate: string) {
        this.renameTemplate = renameTemplate;
        return this;
    }
    transform(curValue: any) {
        if(!this.renameTemplate) return curValue;
        return this.renameTemplate.replace('${cv}', curValue);
    }
}

function toCol(nameOrCol: string | Col): Col {
    if(nameOrCol instanceof Col) {
        return nameOrCol;
    } else {
        return new Col(nameOrCol);
    }
}

export function col(name: string) {
    return new Col(name);
}

export const aggFn = {
    sum: (column: string) => (df: DataFrame): [string, number] => {
        const { rows } = df;
        const key = `sum(${column})`;
        const value = rows.reduce((prev, row) => prev + (row[column] || 0), 0);
        return [key, value];
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