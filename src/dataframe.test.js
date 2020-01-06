import { DataFrame, GroupDataFrame, gRows, aggFn } from "./dataframe";


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
    const df = new DataFrame(data).groupBy('date').pivot('name', aggFn.sum('value'))
    
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', foo: 1, bar: 2 },
        { date: '2020-01-02', foo: 3, bar: 4 }
    ])
})

test('groupDataframe pivot agg value', () => {
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