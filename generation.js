const COLUMN_NAMES = [
  "Index",
  "First Name",
  "Last Name",
  "Age",
  "Column5",
  "Column6",
  "Column7",
  "Column8",
  "Column9",
  "Column10",
  "Column11",
  "Column12",
  "Column13",
  "Column14",
  "Column15",
];

const COLUMN_KEYS = [
  "index",
  "firstName",
  "lastName",
  "age",
  "column5",
  "column6",
  "column7",
  "column8",
  "column9",
  "column10",
  "column11",
  "column12",
  "column13",
  "column14",
  "column15",
];

const BASE_COLUMN_COUNT = COLUMN_KEYS.length;
const NUMERIC_CACHE_OFFSET = BASE_COLUMN_COUNT;
const NUMERIC_TOTAL_WITH_CACHE = BASE_COLUMN_COUNT + NUMERIC_CACHE_OFFSET;
const COLUMN_INDEX_BY_KEY = Object.freeze(
  COLUMN_KEYS.reduce((acc, key, index) => {
    acc[key] = index;
    return acc;
  }, {})
);

const FIRST_NAMES = [
  "James",
  "Mary",
  "John",
  "Patricia",
  "Robert",
  "Jennifer",
  "Michael",
  "Linda",
  "William",
  "Elizabeth",
  "David",
  "Barbara",
  "Richard",
  "Susan",
  "Joseph",
  "Jessica",
  "Thomas",
  "Sarah",
  "Charles",
  "Karen",
  "Christopher",
  "Nancy",
  "Daniel",
  "Lisa",
  "Matthew",
  "Betty",
  "Anthony",
  "Margaret",
  "Mark",
  "Sandra",
  "Donald",
  "Ashley",
  "Steven",
  "Kimberly",
  "Paul",
  "Emily",
  "Andrew",
  "Donna",
  "Joshua",
  "Michelle",
  "Kenneth",
  "Dorothy",
  "Kevin",
  "Carol",
  "Brian",
  "Amanda",
  "George",
  "Melissa",
  "Edward",
  "Deborah",
  "Jason",
  "Stephanie",
  "Jeffrey",
  "Rebecca",
  "Ryan",
  "Sharon",
  "Jacob",
  "Cynthia",
  "Gary",
  "Kathleen",
  "Nicholas",
  "Shirley",
  "Eric",
  "Amy",
  "Jonathan",
  "Angela",
  "Stephen",
  "Helen",
  "Larry",
  "Anna",
  "Justin",
  "Brenda",
  "Scott",
  "Pamela",
  "Brandon",
  "Nicole",
  "Frank",
  "Samantha",
  "Gregory",
  "Katherine",
  "Raymond",
  "Christine",
  "Benjamin",
  "Debra",
  "Samuel",
  "Rachel",
  "Patrick",
  "Catherine",
  "Alexander",
  "Carolyn",
  "Jack",
  "Janet",
  "Dennis",
  "Ruth",
  "Jerry",
  "Maria",
  "Tyler",
  "Heather",
  "Aaron",
  "Diane",
  "Ethan",
  "Megan",
  "Austin",
  "Lauren",
  "Zachary",
  "Victoria",
  "Adam",
  "Olivia",
  "Nathan",
  "Sophia",
  "Jose",
  "Isabella",
  "Charles",
  "Mia",
  "Thomas",
  "Emily",
  "Jordan",
  "Madison",
  "Cameron",
  "Avery",
  "Hunter",
  "Ella",
  "Christian",
  "Scarlett",
  "Aidan",
  "Grace",
  "Evan",
  "Chloe",
  "Isaac",
  "Lily",
  "Luke",
  "Hannah",
  "Mason",
  "Aria",
  "Jayden",
  "Zoe",
  "Gabriel",
  "Layla",
  "Caleb",
  "Riley",
  "Dylan",
  "Nora",
  "Henry",
  "Lillian",
  "Owen",
  "Addison",
  "Wyatt",
  "Aubrey",
  "Jack",
  "Eleanor",
  "Sebastian",
  "Stella",
  "Julian",
  "Natalie",
  "Levi",
  "Hazel",
  "Isaiah",
  "Violet",
  "Landon",
  "Aurora",
  "David",
  "Savannah",
  "Andrew",
  "Penelope",
  "Jaxon",
  "Brooklyn",
  "Eli",
  "Paisley",
  "Aaron",
  "Claire",
  "Christopher",
  "Skylar",
  "Joshua",
  "Lucy",
  "Nolan",
  "Anna",
  "Adrian",
  "Samantha",
  "Carter",
  "Kennedy",
  "Asher",
  "Sadie",
  "Leo",
  "Allison",
  "Jeremiah",
  "Gabriella",
  "Hudson",
  "Ariana",
  "Lincoln",
  "Alice",
  "Grayson",
  "Madeline",
  "Jace",
  "Ruby",
  "Mateo",
  "Eva",
  "Jason",
  "Autumn",
  "Ezra",
  "Quinn",
  "Parker",
  "Piper",
  "Josiah",
  "Sophie",
  "Carson",
  "Lydia"
];

const LAST_NAMES = [
  "Smith",
  "Johnson",
  "Williams",
  "Brown",
  "Jones",
  "Miller",
  "Davis",
  "Wilson",
  "Anderson",
  "Thomas",
  "Taylor",
  "Moore",
  "Jackson",
  "Martin",
  "White",
  "Clark",
  "Lewis",
  "Robinson",
  "Walker",
  "Young",
  "Allen",
  "King",
  "Wright",
  "Scott",
  "Hill",
  "Green",
  "Adams",
  "Nelson",
  "Baker",
  "Hall",
  "Campbell",
  "Mitchell",
  "Carter",
  "Roberts",
  "Phillips",
  "Evans",
  "Turner",
  "Parker",
  "Edwards",
  "Collins",
  "Stewart",
  "Morris",
  "Murphy",
  "Cook",
  "Rogers",
  "Morgan",
  "Cooper",
  "Peterson",
  "Bailey",
  "Reed",
  "Kelly",
  "Howard",
  "Cox",
  "Ward",
  "Richardson",
  "Watson",
  "Brooks",
  "Wood",
  "James",
  "Bennett",
  "Gray",
  "Hughes",
  "Price",
  "Sanders",
  "Myers",
  "Long",
  "Ross",
  "Foster",
  "Harrison",
  "Graham",
  "Fisher",
  "Hansen",
  "Grant",
  "Hart",
  "Spencer",
  "Gardner",
  "Payne",
  "Pierce",
  "Berry",
  "Matthews",
  "Arnold",
  "Wagner",
  "Willis",
  "Ray",
  "Watkins",
  "Olson",
  "Carroll",
  "Duncan",
  "Snyder",
  "Hart",
  "Cunningham",
  "Bradley",
  "Lane",
  "Andrews",
  "Ruiz",
  "Harper",
  "Fox",
  "Riley",
  "Armstrong",
  "Carpenter",
  "Weaver",
  "Greene",
  "Lawrence",
  "Elliott",
  "Chavez",
  "Sims",
  "Austin",
  "Peters",
  "Kelley",
  "Franklin",
  "Lawson",
  "Fields",
  "Gutierrez",
  "Ryan",
  "Schmidt",
  "Carr",
  "Vasquez",
  "Castillo",
  "Wheeler",
  "Chapman",
  "Oliver",
  "Montgomery",
  "Richards",
  "Williamson",
  "Johnston",
  "Banks",
  "Meyer",
  "Bishop",
  "McCoy",
  "Howell",
  "Alvarez",
  "Morrison",
  "Hansen",
  "Fernandez",
  "Garza",
  "Harvey",
  "Little",
  "Burton",
  "Stanley",
  "Nguyen",
  "George",
  "Jacobs",
  "Reid",
  "Kim",
  "Fuller",
  "Lynch",
  "Dean",
  "Gilbert",
  "Garrett",
  "Romero",
  "Welch",
  "Larson",
  "Frazier",
  "Burke",
  "Hanson",
  "Day",
  "Mendoza",
  "Moreno",
  "Bowman",
  "Medina",
  "Fowler",
  "Brewer",
  "Hoffman",
  "Carlson",
  "Silva",
  "Pearson",
  "Holland",
  "Douglas",
  "Fleming",
  "Jensen",
  "Vargas",
  "Byrd",
  "Davidson"
];

function randomFrom(values) {
  return values[(Math.random() * values.length) | 0];
}

function randomInt(min, max) {
  return (Math.random() * (max - min + 1) + min) | 0;
}

const CACHE_SUFFIX = "Cache";
const GENERATION_CACHE_KEYS = COLUMN_KEYS;
const DICTIONARY_CARDINALITY_RATIO_LIMIT = 0.2;
const DICTIONARY_MAX_UNIQUE_KEYS = 65535;

function createLowerCacheStore() {
  const store = {};

  for (let i = 0; i < GENERATION_CACHE_KEYS.length; i += 1) {
    store[GENERATION_CACHE_KEYS[i]] = new Map();
  }

  return store;
}

function getCachedLowerValue(cacheStore, key, value) {
  const keyCache = cacheStore[key];
  const cached = keyCache.get(value);
  if (cached !== undefined) {
    return cached;
  }

  const normalized = String(value).toLowerCase();
  keyCache.set(value, normalized);
  return normalized;
}

function addCacheMetric(metrics, deltaMs) {
  if (!metrics) {
    return;
  }

  metrics.cacheGenerationMs += deltaMs;
}

function createEmptyLowerDictionaryPostings() {
  return Object.create(null);
}

function normalizeDictionaryKey(value) {
  return String(value).toLowerCase();
}

function isLikelyStringColumnByIndex(columnIndex) {
  const key = COLUMN_KEYS[columnIndex];
  return key === "firstName" || key === "lastName";
}

function detectBaseColumnKind(sampleValue, columnIndex) {
  if (typeof sampleValue === "number" || !isLikelyStringColumnByIndex(columnIndex)) {
    return Number.isInteger(sampleValue) ? "int" : "float";
  }

  return "string";
}

function createBaseColumnStore(baseKind, rowCount) {
  if (baseKind === "int") {
    return new Int32Array(rowCount);
  }

  if (baseKind === "float") {
    return new Float64Array(rowCount);
  }

  return new Array(rowCount);
}

function createColumnDictionaryBuilder(rowCount, disableOnRatio) {
  return {
    active: rowCount > 0,
    disabledByUniqueLimit: false,
    disableOnRatio,
    maxUniqueByRatio: Math.floor(rowCount * DICTIONARY_CARDINALITY_RATIO_LIMIT),
    uniqueCount: 0,
    ids: rowCount > 0 ? new Uint16Array(rowCount) : new Uint16Array(0),
    dictionary: [],
    lowerValues: [],
    lowerDictionary: createEmptyLowerDictionaryPostings(),
    keyToId: new Map(),
  };
}

function disableColumnDictionaryBuilder(builder, overUniqueLimit) {
  builder.active = false;
  builder.disabledByUniqueLimit = overUniqueLimit === true;
  builder.uniqueCount = 0;
  builder.ids = null;
  builder.dictionary = [];
  builder.lowerValues = [];
  builder.lowerDictionary = createEmptyLowerDictionaryPostings();
  builder.keyToId = null;
}

function recordColumnDictionaryValue(builder, value, rowIndex) {
  if (!builder.active) {
    return;
  }

  const normalized = normalizeDictionaryKey(value);
  const keyToId = builder.keyToId;
  let valueId = keyToId.get(normalized);

  if (valueId === undefined) {
    const nextUniqueCount = builder.uniqueCount + 1;

    if (nextUniqueCount > DICTIONARY_MAX_UNIQUE_KEYS) {
      disableColumnDictionaryBuilder(builder, true);
      return;
    }

    if (builder.disableOnRatio && nextUniqueCount > builder.maxUniqueByRatio) {
      disableColumnDictionaryBuilder(builder, false);
      return;
    }

    valueId = builder.dictionary.length;
    keyToId.set(normalized, valueId);
    builder.dictionary.push(value);
    builder.lowerValues.push(normalized);
    builder.lowerDictionary[normalized] = [];
    builder.uniqueCount = nextUniqueCount;
  }

  builder.ids[rowIndex] = valueId;
  builder.lowerDictionary[normalized].push(rowIndex);
}

function createNumericColumnBuildState(sampleValues, rowCount) {
  const baseKinds = new Array(BASE_COLUMN_COUNT);
  const baseColumns = new Array(BASE_COLUMN_COUNT);
  const dictionaryBuilders = new Array(BASE_COLUMN_COUNT);

  for (let c = 0; c < BASE_COLUMN_COUNT; c += 1) {
    const baseKind = detectBaseColumnKind(sampleValues[c], c);
    baseKinds[c] = baseKind;
    baseColumns[c] = createBaseColumnStore(baseKind, rowCount);
    dictionaryBuilders[c] = createColumnDictionaryBuilder(rowCount, baseKind !== "string");
  }

  return {
    baseKinds,
    baseColumns,
    dictionaryBuilders,
  };
}

function finalizeNumericColumnBuildState(state, rowCount) {
  const columns = new Array(BASE_COLUMN_COUNT);
  const columnKinds = new Array(BASE_COLUMN_COUNT);
  const dictionaries = new Array(BASE_COLUMN_COUNT);
  const lowerDictionaries = new Array(BASE_COLUMN_COUNT);
  const lowerDictionaryValues = new Array(BASE_COLUMN_COUNT);

  for (let c = 0; c < BASE_COLUMN_COUNT; c += 1) {
    const baseKind = state.baseKinds[c];
    const baseColumn = state.baseColumns[c];
    const builder = state.dictionaryBuilders[c];
    const canUseCompactDictionary =
      builder.active &&
      builder.uniqueCount > 0 &&
      builder.uniqueCount <= DICTIONARY_MAX_UNIQUE_KEYS &&
      builder.uniqueCount / Math.max(1, rowCount) <= DICTIONARY_CARDINALITY_RATIO_LIMIT;

    if (canUseCompactDictionary) {
      columnKinds[c] = "stringId";
      columns[c] = builder.ids;
      dictionaries[c] = builder.dictionary;
      lowerDictionaries[c] = builder.lowerDictionary;
      lowerDictionaryValues[c] = builder.lowerValues;
      continue;
    }

    if (
      baseKind === "string" &&
      builder.active &&
      builder.uniqueCount > 0 &&
      builder.uniqueCount <= DICTIONARY_MAX_UNIQUE_KEYS &&
      builder.ids !== null
    ) {
      const ids = new Uint32Array(rowCount);
      ids.set(builder.ids);
      columnKinds[c] = "stringId";
      columns[c] = ids;
      dictionaries[c] = builder.dictionary;
      lowerDictionaries[c] = builder.lowerDictionary;
      lowerDictionaryValues[c] = builder.lowerValues;
      continue;
    }

    columnKinds[c] = baseKind;
    columns[c] = baseColumn;
    dictionaries[c] = [];
    lowerDictionaries[c] = createEmptyLowerDictionaryPostings();
    lowerDictionaryValues[c] = [];
  }

  return {
    columns,
    columnKinds,
    dictionaries,
    lowerDictionaries,
    lowerDictionaryValues,
  };
}

function getGeneratedColumnValue(col, index, rowCount) {
  if (col === 6) {
    return rowCount - index;
  }

  if (col === 7) {
    return index * 2;
  }

  if (col === 8) {
    return index * 2 - 1;
  }

  return randomInt(0, rowCount);
}

function generateRowsWithoutCache(rowCount, options) {
  const generationOptions = options || {};
  const startIndex = Number.isFinite(generationOptions.startIndex)
    ? Math.max(1, Math.floor(generationOptions.startIndex))
    : 1;
  const totalRowCount = Number.isFinite(generationOptions.totalRowCount)
    ? Math.max(1, Math.floor(generationOptions.totalRowCount))
    : rowCount;
  const rows = new Array(rowCount);

  for (let i = 0; i < rowCount; i += 1) {
    const index = startIndex + i;
    const firstName = randomFrom(FIRST_NAMES);
    const lastName = randomFrom(LAST_NAMES);
    const age = randomInt(18, 99);

    const row = {
      index,
      firstName,
      lastName,
      age
    };

    for (let col = 5; col <= 15; col += 1) {
      const columnKey = `column${col}`;
      const value = getGeneratedColumnValue(col, index, totalRowCount);
      row[columnKey] = value;
    }

    rows[i] = row;
  }

  return rows;
}

function attachCachesToRows(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return rows;
  }

  const lowerCacheStore = createLowerCacheStore();

  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i];

    for (let j = 0; j < COLUMN_KEYS.length; j += 1) {
      const key = COLUMN_KEYS[j];
      row[`${key}${CACHE_SUFFIX}`] = getCachedLowerValue(
        lowerCacheStore,
        key,
        row[key]
      );
    }
  }

  return rows;
}

function generateRows(rowCount) {
  const rows = generateRowsWithoutCache(rowCount);
  attachCachesToRows(rows);
  return rows;
}

function createColumnarColumns(rowCount) {
  const columns = {
    index: new Int32Array(rowCount),
    firstName: new Array(rowCount),
    lastName: new Array(rowCount),
    age: new Uint8Array(rowCount),
    column5: new Int32Array(rowCount),
    column6: new Int32Array(rowCount),
    column7: new Int32Array(rowCount),
    column8: new Int32Array(rowCount),
    column9: new Int32Array(rowCount),
    column10: new Int32Array(rowCount),
    column11: new Int32Array(rowCount),
    column12: new Int32Array(rowCount),
    column13: new Int32Array(rowCount),
    column14: new Int32Array(rowCount),
    column15: new Int32Array(rowCount),
  };

  columns.indexCache = new Array(rowCount);
  columns.firstNameCache = new Array(rowCount);
  columns.lastNameCache = new Array(rowCount);
  columns.ageCache = new Array(rowCount);
  columns.column5Cache = new Array(rowCount);
  columns.column6Cache = new Array(rowCount);
  columns.column7Cache = new Array(rowCount);
  columns.column8Cache = new Array(rowCount);
  columns.column9Cache = new Array(rowCount);
  columns.column10Cache = new Array(rowCount);
  columns.column11Cache = new Array(rowCount);
  columns.column12Cache = new Array(rowCount);
  columns.column13Cache = new Array(rowCount);
  columns.column14Cache = new Array(rowCount);
  columns.column15Cache = new Array(rowCount);

  return columns;
}

function generateColumnarData(rowCount) {
  const columns = createColumnarColumns(rowCount);
  const lowerCacheStore = createLowerCacheStore();

  for (let i = 0; i < rowCount; i += 1) {
    const index = i + 1;
    const firstName = randomFrom(FIRST_NAMES);
    const lastName = randomFrom(LAST_NAMES);
    const age = randomInt(18, 99);

    columns.index[i] = index;
    columns.firstName[i] = firstName;
    columns.lastName[i] = lastName;
    columns.age[i] = age;

    columns.indexCache[i] = getCachedLowerValue(lowerCacheStore, "index", index);
    columns.firstNameCache[i] = getCachedLowerValue(
      lowerCacheStore,
      "firstName",
      firstName
    );
    columns.lastNameCache[i] = getCachedLowerValue(
      lowerCacheStore,
      "lastName",
      lastName
    );
    columns.ageCache[i] = getCachedLowerValue(lowerCacheStore, "age", age);

    for (let col = 5; col <= 15; col += 1) {
      const columnKey = `column${col}`;
      const value = getGeneratedColumnValue(col, index, rowCount);
      columns[columnKey][i] = value;

      columns[`${columnKey}${CACHE_SUFFIX}`][i] = getCachedLowerValue(
        lowerCacheStore,
        columnKey,
        value
      );
    }
  }

  return {
    rowCount,
    columns,
  };
}

function deriveColumnarDataFromRows(rows, options) {
  const metrics = options && options.metrics;
  const rowCount = Array.isArray(rows) ? rows.length : 0;
  const columns = createColumnarColumns(rowCount);
  const lowerCacheStore = createLowerCacheStore();

  if (metrics) {
    metrics.cacheGenerationMs = 0;
  }

  const cacheGenerationStartMs = metrics ? performance.now() : 0;

  for (let i = 0; i < rowCount; i += 1) {
    const row = rows[i];

    for (let j = 0; j < COLUMN_KEYS.length; j += 1) {
      const key = COLUMN_KEYS[j];
      const value = row[key];
      columns[key][i] = value;

      const cacheKey = `${key}${CACHE_SUFFIX}`;
      const cachedValue = row[cacheKey];
      columns[cacheKey][i] =
        cachedValue !== undefined
          ? cachedValue
          : getCachedLowerValue(lowerCacheStore, key, value);
    }
  }

  if (metrics) {
    addCacheMetric(metrics, performance.now() - cacheGenerationStartMs);
  }

  return {
    rowCount,
    columns,
  };
}

function deriveObjectAndNumericColumnarFromRows(rows, options) {
  const metrics = options && options.metrics;
  const rowCount = Array.isArray(rows) ? rows.length : 0;
  const objectColumns = createColumnarColumns(rowCount);
  const cacheColumns = new Array(BASE_COLUMN_COUNT);
  const lowerCacheStore = createLowerCacheStore();
  const sampleValues = new Array(BASE_COLUMN_COUNT);

  if (metrics) {
    metrics.cacheGenerationMs = 0;
  }

  for (let c = 0; c < BASE_COLUMN_COUNT; c += 1) {
    sampleValues[c] = rowCount > 0 ? rows[0][COLUMN_KEYS[c]] : undefined;
    cacheColumns[c] = new Array(rowCount);
  }

  const numericBuildState = createNumericColumnBuildState(sampleValues, rowCount);
  const baseColumns = numericBuildState.baseColumns;
  const dictionaryBuilders = numericBuildState.dictionaryBuilders;
  const cacheGenerationStartMs = metrics ? performance.now() : 0;

  for (let r = 0; r < rowCount; r += 1) {
    const row = rows[r];

    for (let c = 0; c < BASE_COLUMN_COUNT; c += 1) {
      const key = COLUMN_KEYS[c];
      const cacheKey = `${key}${CACHE_SUFFIX}`;
      const value = row[key];

      objectColumns[key][r] = value;

      const cachedValue = row[cacheKey];
      const normalizedValue =
        cachedValue !== undefined
          ? cachedValue
          : getCachedLowerValue(lowerCacheStore, key, value);

      objectColumns[cacheKey][r] = normalizedValue;
      cacheColumns[c][r] = normalizedValue;
      baseColumns[c][r] = value;
      recordColumnDictionaryValue(dictionaryBuilders[c], value, r);
    }
  }

  if (metrics) {
    addCacheMetric(metrics, performance.now() - cacheGenerationStartMs);
  }

  const finalizedNumeric = finalizeNumericColumnBuildState(
    numericBuildState,
    rowCount
  );

  return {
    objectColumnarData: {
      rowCount,
      columns: objectColumns,
    },
    numericColumnarData: {
      rowCount,
      columnCount: BASE_COLUMN_COUNT,
      baseColumnCount: BASE_COLUMN_COUNT,
      cacheOffset: NUMERIC_CACHE_OFFSET,
      hasCacheColumns: true,
      columns: finalizedNumeric.columns,
      columnKinds: finalizedNumeric.columnKinds,
      dictionaries: finalizedNumeric.dictionaries,
      lowerDictionaries: finalizedNumeric.lowerDictionaries,
      lowerDictionaryValues: finalizedNumeric.lowerDictionaryValues,
      cacheColumns,
    },
  };
}

function deriveNumericRowsFromRows(rows, options) {
  const metrics = options && options.metrics;
  const rowCount = Array.isArray(rows) ? rows.length : 0;
  const totalColumnCount = NUMERIC_TOTAL_WITH_CACHE;
  const numericRows = new Array(rowCount);
  const lowerCacheStore = createLowerCacheStore();

  if (metrics) {
    metrics.cacheGenerationMs = 0;
  }

  const cacheGenerationStartMs = metrics ? performance.now() : 0;

  for (let i = 0; i < rowCount; i += 1) {
    const sourceRow = rows[i];
    const numericRow = new Array(totalColumnCount);

    for (let j = 0; j < BASE_COLUMN_COUNT; j += 1) {
      const key = COLUMN_KEYS[j];
      const value = sourceRow[key];
      numericRow[j] = value;

      const cacheKey = `${key}${CACHE_SUFFIX}`;
      const cachedValue = sourceRow[cacheKey];
      numericRow[j + NUMERIC_CACHE_OFFSET] =
        cachedValue !== undefined
          ? cachedValue
          : getCachedLowerValue(lowerCacheStore, key, value);
    }

    numericRows[i] = numericRow;
  }

  if (metrics) {
    addCacheMetric(metrics, performance.now() - cacheGenerationStartMs);
  }

  return {
    rowCount,
    rows: numericRows,
    baseColumnCount: BASE_COLUMN_COUNT,
    cacheOffset: NUMERIC_CACHE_OFFSET,
  };
}

function deriveNumericColumnarDataFromNumericRows(numericRows) {
  const rowCount = Array.isArray(numericRows) ? numericRows.length : 0;
  const hasCacheColumns =
    rowCount > 0 &&
    Array.isArray(numericRows[0]) &&
    numericRows[0].length > BASE_COLUMN_COUNT;
  const sampleValues = new Array(BASE_COLUMN_COUNT);

  for (let c = 0; c < BASE_COLUMN_COUNT; c += 1) {
    sampleValues[c] = rowCount > 0 ? numericRows[0][c] : undefined;
  }

  const numericBuildState = createNumericColumnBuildState(sampleValues, rowCount);
  const baseColumns = numericBuildState.baseColumns;
  const dictionaryBuilders = numericBuildState.dictionaryBuilders;

  const cacheColumns = hasCacheColumns ? new Array(BASE_COLUMN_COUNT) : null;
  if (cacheColumns !== null) {
    for (let c = 0; c < BASE_COLUMN_COUNT; c += 1) {
      cacheColumns[c] = new Array(rowCount);
    }
  }

  for (let r = 0; r < rowCount; r += 1) {
    const row = numericRows[r];

    for (let c = 0; c < BASE_COLUMN_COUNT; c += 1) {
      const value = row[c];
      baseColumns[c][r] = value;
      recordColumnDictionaryValue(dictionaryBuilders[c], value, r);

      if (cacheColumns !== null) {
        cacheColumns[c][r] = row[c + NUMERIC_CACHE_OFFSET];
      }
    }
  }

  const finalizedNumeric = finalizeNumericColumnBuildState(
    numericBuildState,
    rowCount
  );

  return {
    rowCount,
    columnCount: BASE_COLUMN_COUNT,
    baseColumnCount: BASE_COLUMN_COUNT,
    cacheOffset: NUMERIC_CACHE_OFFSET,
    hasCacheColumns,
    columns: finalizedNumeric.columns,
    columnKinds: finalizedNumeric.columnKinds,
    dictionaries: finalizedNumeric.dictionaries,
    lowerDictionaries: finalizedNumeric.lowerDictionaries,
    lowerDictionaryValues: finalizedNumeric.lowerDictionaryValues,
    cacheColumns,
  };
}

function deriveObjectAndNumericColumnarFromNumericRows(numericRows) {
  const rowCount = Array.isArray(numericRows) ? numericRows.length : 0;
  const hasCacheColumns =
    rowCount > 0 &&
    Array.isArray(numericRows[0]) &&
    numericRows[0].length > BASE_COLUMN_COUNT;

  const objectColumns = createColumnarColumns(rowCount);
  const sampleValues = new Array(BASE_COLUMN_COUNT);

  for (let c = 0; c < BASE_COLUMN_COUNT; c += 1) {
    sampleValues[c] = rowCount > 0 ? numericRows[0][c] : undefined;
  }

  const numericBuildState = createNumericColumnBuildState(sampleValues, rowCount);
  const baseColumns = numericBuildState.baseColumns;
  const dictionaryBuilders = numericBuildState.dictionaryBuilders;

  const cacheColumns = hasCacheColumns ? new Array(BASE_COLUMN_COUNT) : null;
  if (cacheColumns !== null) {
    for (let c = 0; c < BASE_COLUMN_COUNT; c += 1) {
      cacheColumns[c] = new Array(rowCount);
    }
  }

  for (let r = 0; r < rowCount; r += 1) {
    const row = numericRows[r];

    for (let c = 0; c < BASE_COLUMN_COUNT; c += 1) {
      const key = COLUMN_KEYS[c];
      const baseValue = row[c];
      const cacheKey = `${key}${CACHE_SUFFIX}`;
      objectColumns[key][r] = baseValue;

      let cacheValue = "";
      if (hasCacheColumns) {
        cacheValue = row[c + NUMERIC_CACHE_OFFSET];
      } else {
        cacheValue = String(baseValue).toLowerCase();
      }

      objectColumns[cacheKey][r] = cacheValue;
      if (cacheColumns !== null) {
        cacheColumns[c][r] = cacheValue;
      }
      baseColumns[c][r] = baseValue;
      recordColumnDictionaryValue(dictionaryBuilders[c], baseValue, r);
    }
  }

  const finalizedNumeric = finalizeNumericColumnBuildState(
    numericBuildState,
    rowCount
  );

  return {
    objectColumnarData: {
      rowCount,
      columns: objectColumns,
    },
    numericColumnarData: {
      rowCount,
      columnCount: BASE_COLUMN_COUNT,
      baseColumnCount: BASE_COLUMN_COUNT,
      cacheOffset: NUMERIC_CACHE_OFFSET,
      hasCacheColumns,
      columns: finalizedNumeric.columns,
      columnKinds: finalizedNumeric.columnKinds,
      dictionaries: finalizedNumeric.dictionaries,
      lowerDictionaries: finalizedNumeric.lowerDictionaries,
      lowerDictionaryValues: finalizedNumeric.lowerDictionaryValues,
      cacheColumns,
    },
  };
}

function formatCount(value) {
  return value.toLocaleString("de-DE");
}

const fastTableGenerationApi = {
  COLUMN_NAMES,
  COLUMN_KEYS,
  COLUMN_INDEX_BY_KEY,
  BASE_COLUMN_COUNT,
  NUMERIC_CACHE_OFFSET,
  generateRowsWithoutCache,
  attachCachesToRows,
  generateRows,
  generateColumnarData,
  deriveColumnarDataFromRows,
  deriveObjectAndNumericColumnarFromRows,
  deriveNumericRowsFromRows,
  deriveNumericColumnarDataFromNumericRows,
  deriveObjectAndNumericColumnarFromNumericRows,
  formatCount,
};

if (typeof window !== "undefined") {
  window.fastTableGeneration = fastTableGenerationApi;
}

if (typeof self !== "undefined") {
  self.fastTableGeneration = fastTableGenerationApi;
}
