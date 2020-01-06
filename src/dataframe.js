const flatMap = (f,xs) =>
  xs.map(f).reduce((x,y) => x.concat(y), [])

Array.prototype.flatMap = function(f) {
  return flatMap(f,this)
}


export const gRows = Symbol('gRows');

// [ Row, Row, Row ]
export class DataFrame {
    constructor(rows) {
        this.rows = rows;
        this.values = {};
    }

    groupBy(column) {
        const g = this.rows.reduce((g, row) => {
            if(g[row[column]]) {
                g[row[column]].push(row);
            } else {
                g[row[column]] = [row];
            }
            return g;
        }, {});

        const g2 = Object.entries(g).map(([colValue, rows])=>
            ({ [column]: rows[0][column], [gRows]: new DataFrame(rows) })
        );

        return new GroupDataFrame(this, g2);
    }

    groupBys(columns) {
        for (let column of columns) {
            const g = this.groupBy(column);
        }
    }

    select(expr) {
        const fn = parser(lexer(expr));

        const rows = this.rows.map(row => {
            return { [expr]: fn(row) }
        });

        return new DataFrame(rows);
    }

    agg(fn) {
        return fn(this)[1];
    }

    // the order may need to reconsider.
    getValues(column) {
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
}

// [
//   { pId: 'P1', rows: DataFrame }
//   { pId: 'P2', rows: DataFrame }
// ]
export class GroupDataFrame {
    constructor(dataframe, gData) {
        this.df = dataframe;
        this.gData = gData;
    }

    // 因为不知道怎么处理嵌套groupby, 所以我把group的列拍平了.
    // return GroupDataFrame
    // [
    //   { pId: 'P1', country: 'US', rows: [Row] }
    //   { pId: 'P1', country: 'UK', rows: [Row, Row] }
    // ]
    groupBy(column) {
        const gData = this.gData.flatMap(gRow => {
            const { [gRows]: dataFrame, ...rest } = gRow;
            return dataFrame.groupBy(column).gData.map(gRow => ({ ...rest, ...gRow }));
        });
        return new GroupDataFrame(this.df, gData);
    }
    
    // pivot
    // 将columnToPivot这一列数据，变成新的列，数据聚合内容
    pivot(col, aggFns) {
        // 提前遍历获得所有新列名，保证行数据的完整性 test: groupDataframe pivot with uncompelete data
        if (typeof col === 'string') {
            col = new Col(col);
        }
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
    _pivotAgg(columnToPivot, newColumnNames, aggFn, newColumnNameMappings) {
        return this.gData.map(gRow => {
            const { [gRows]: df, ...groups } = gRow;
            // { name: 'foo',  [gRows]: } { name: 'bar',  [gRows]: }
            // { foo: aggFn(foo's rows), bar: aggFn(bar's rows)}
            const gdf = df.groupBy(columnToPivot);

            const newRowInit = newColumnNames.reduce((ret, name) => {
                ret[name] = 0;
                return ret;
            }, {});

            let newRow = gdf.gData.reduce((ret, { [columnToPivot]: newColumnName, [gRows]: df }) => {
                ret[newColumnName] = df.agg(aggFn);
                return ret;
            }, newRowInit);

            // if col as exists
            if(newColumnNameMappings) {
                newRow = newColumnNames.reduce((ret, colName) => {
                    ret[newColumnNameMappings[colName]] = newRow[colName];
                    return ret;
                }, {});
            }

            return {...newRow, ...groups };
        });
    }

    agg(fn) {
        const data = this.gData.map(gRow => {
            const { [gRows]: dataFrame, ...rest } = gRow;
            const [key, value] = fn(dataFrame);
            return { ...rest, [key]: value };
        });
        return new DataFrame(data);
    }
}

export class Col {
    constructor(name) {
        this.name = name;
    }
    as(renameTemplate) {
        this.renameTemplate = renameTemplate;
        return this;
    }
    transform(curValue) {
        if(!this.renameTemplate) return curValue;
        return this.renameTemplate.replace('${cv}', curValue);
    }
}

export function col(name) {
    return new Col(name);
}

export const aggFn = {
    sum: (column) => ({ rows }) => {
        const key = `sum(${column})`;
        const value = rows.reduce((prev, row) => prev + (row[column] || 0), 0);
        return [key, value];
    }
};

function lexer(expr) {
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

function parser(tokens) {
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
