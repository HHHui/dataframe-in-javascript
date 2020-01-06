import { DataFrame, GroupDataFrame, gRows, aggFn, col } from "./dataframe";


const data = [
    { date: '2020-01-01', name: 'foo', value: 1 },
    { date: '2020-01-01', name: 'bar', value: 2 },
    { date: '2020-01-02', name: 'foo', value: 3 },
    { date: '2020-01-02', name: 'bar', value: 4 }
]

test('dataframe groupBy', () => {
    const gdf = new DataFrame(data).groupBy('date');

    expect(gdf).toBeInstanceOf(GroupDataFrame);

    const { [gRows]: df, ...groups } = gdf.gData[0];
    expect(groups).toStrictEqual({ date: '2020-01-01' });
    expect(df).toBeInstanceOf(DataFrame);
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', name: 'foo', value: 1 },
        { date: '2020-01-01', name: 'bar', value: 2 }
    ]);
});

xtest('dataframe groupBy with calc', () => {
    const data = [
        { date: '2020-01-01 00', value: 1 },
        { date: '2020-01-01 01', value: 2 },
        { date: '2020-01-02 00', value: 3 },
        { date: '2020-01-02 01', value: 4 }
    ]

    const df = new DataFrame(data).groupBy(col('date').expr(v => v.substr(0, 10))).agg(aggFn.sum('value'));

    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', value: 3 },
        { date: '2020-01-02', value: 7 }
    ]);
});

test('dataframe getValues', () => {
    const data = [
        { date: '2020-01-01', name: 'foo' },
        { date: '2020-01-01', name: undefined },
        { date: '2020-01-01', name: null },
        { date: '2020-01-02', name: 'bar' }
    ]
    const df = new DataFrame(data);
    const values = df.getValues('name');
    expect(new Set(values)).toEqual(new Set(['foo', 'bar']));
    
    expect(values).toBe(df.values['name'])
})

// select expr
test('dataframe select one cloumn', () => {
    const df = new DataFrame(data).select('name');
    expect(df.rows).toStrictEqual([
        { name: 'foo' },
        { name: 'bar' },
        { name: 'foo' },
        { name: 'bar' }
    ]);
})

test('dataframe select support one column +-*/', () => {
    const df = new DataFrame(data).select('value + 1');
    expect(df.rows).toStrictEqual([
        { 'value + 1': 2 },
        { 'value + 1': 3 },
        { 'value + 1': 4 },
        { 'value + 1': 5 }
    ]);
})

xtest('dataframe select cloumns', () => {
    const df = new DataFrame(data).select('name', 'value');
    expect(df.rows).toStrictEqual([
        { value: 1, name: 'foo' },
        { value: 2, name: 'bar' },
        { value: 3, name: 'foo' },
        { value: 4, name: 'bar' }
    ]);
})
// select expr end

test('groupDataframe groupBy', () => {
    const gdf = new DataFrame(data).groupBy('date').groupBy('name');
    
    expect(gdf).toBeInstanceOf(GroupDataFrame);

    const { [gRows]: df, ...groups } = gdf.gData[0];
    expect(groups).toStrictEqual({ date: '2020-01-01', name: 'foo' });
    expect(df).toBeInstanceOf(DataFrame);
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', name: 'foo', value: 1 },
    ]);
});

test('groupDataframe pivot', () => {
    const data = [
        { date: '2020-01-01', name: 'foo', value: 1 },
        { date: '2020-01-01', name: 'foo', value: 2 },
        { date: '2020-01-01', name: 'bar', value: 3 },
        { date: '2020-01-01', name: 'bar', value: 4 }
    ]

    const df = new DataFrame(data).groupBy('date').pivot('name', aggFn.sum('value'))
    
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', foo: 3, bar: 7 },
    ])
})

test('groupDataframe pivot rename', () => {
    const df = new DataFrame(data).groupBy('date').pivot(col('name').as('${cv}Value'), aggFn.sum('value'))

    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', fooValue: 1, barValue: 2 },
        { date: '2020-01-02', fooValue: 3, barValue: 4 },
    ])
})

test('groupDataframe pivot with uncompelete data', () => {

    const data = [
        { date: '2020-01-01', name: 'foo', value: 1 },
        { date: '2020-01-02', name: 'bar', value: 4 }
    ]

    const df = new DataFrame(data).groupBy('date').pivot('name', aggFn.sum('value'))
    
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', foo: 1, bar: 0 },
        { date: '2020-01-02', foo: 0, bar: 4 }
    ])
})

test('groupDataframe pivot with multiple aggregation function', () => {
    const data = [
        { date: '2020-01-01', name: 'foo', in: 1, out: 11 },
        { date: '2020-01-01', name: 'bar', in: 2, out: 12 },
        { date: '2020-01-02', name: 'foo', in: 3, out: 13 },
        { date: '2020-01-02', name: 'bar', in: 4, out: 14 },
        { date: '2020-01-02', name: 'bar', in: 6, out: 6 }
    ]

    const [inDf, outDf] = new DataFrame(data).groupBy('date').pivot('name', [aggFn.sum('in'), aggFn.sum('out')])
    
    expect(inDf.rows).toStrictEqual([
        { date: '2020-01-01', foo: 1, bar: 2 },
        { date: '2020-01-02', foo: 3, bar: 10 },
    ])
    expect(outDf.rows).toStrictEqual([
        { date: '2020-01-01', foo: 11, bar: 12 },
        { date: '2020-01-02', foo: 13, bar: 20 },
    ])
})

test('groupDataframe agg sum', () => {
    const df = new DataFrame(data).groupBy('date').agg(aggFn.sum('value'));
    
    expect(df).toBeInstanceOf(DataFrame);
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', 'sum(value)': 3 },
        { date: '2020-01-02', 'sum(value)': 7 }
    ])
})

test('groupDataframe agg sum with data has null value', () => {
    const data = [
        { date: '2020-01-01', value: 1 },
        { date: '2020-01-01' },
        { date: '2020-01-01', value: null },
        { date: '2020-01-02' },
    ]

    const df = new DataFrame(data).groupBy('date').agg(aggFn.sum('value'));
    
    expect(df).toBeInstanceOf(DataFrame);
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', 'sum(value)': 1 },
        { date: '2020-01-02', 'sum(value)': 0 }
    ])
})

test('groupDataframe should have its parent dataframe ref', () => {
    const df = new DataFrame(data);
    const gdf = df.groupBy('date');

    expect(gdf.df).toBe(df);
});