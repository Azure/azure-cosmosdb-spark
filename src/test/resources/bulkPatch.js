        function bulkPatch(batch, pkDef) {
            if (!Array.isArray(batch)) throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.argumentMustBeArray, typeof batch));

            var collection = getContext().getCollection();
            var collectionLink = collection.getSelfLink();

            if (batch.length == 0) {
                setResponse(0);
                return;
            }

            // The count of patched docs, also used as current doc index.
            var patchedCount = 0;

            tryPatch(batch[patchedCount]);

            function tryPatch(patchItem, continuation) {
                var query = sprintf('select * from root r where r.id = \'%s\' and r.%s = \'%s\'', patchItem.id, pkDef, patchItem.pk);
                var requestOptions = { continuation: continuation };

                var isAcceptedQuery = collection.queryDocuments(collectionLink, query, requestOptions, queryCallback);
                if (!isAcceptedQuery) {
                    setResponse(patchedCount);
                    return;
                }
            }

            // Query callback
            function queryCallback(err, results, queryResponseOptions) {
                if (err) throw err;

                let resultIndex = 0;
                if (results.length === 0) {
                    if (queryResponseOptions.continuation) {
                        tryPatch(batch[patchedCount], queryResponseOptions.continuation);
                        return;
                    }
                    else {
                        setResponse(patchedCount, HttpStatusCode.NotFound);
                        return;
                    }
                }
                else {
                    processOneResult();
                }

                patchedCount++;
                if (patchedCount >= batch.length) {
                    // If we have patched all documents, we are done. Just set the response.
                    setResponse(patchedCount);
                } else {
                    // Patch next document.
                    tryPatch(batch[patchedCount]);
                }

                function processOneResult() {
                    var doc = results[resultIndex];
                    var updateOperations = batch[patchedCount].updates;

                    // Apply each update operation to the retrieved document
                    for (var j = 0; j < updateOperations.length; j++) {
                        var operation = updateOperations[j];
                        var isCreate = false;

                        try {
                            doc = handleUpdate(operation, doc, isCreate);
                        } catch (err) {
                            throw err;
                        }
                    }

                    var isAcceptedUpdate = collection.replaceDocument(doc._self, doc, { etag: doc._etag }, updateCallback);
                    if (!isAcceptedUpdate) {
                        setResponse(patchedCount);
                        return;
                    }
                }

                function updateCallback(err) {
                    if (err) {
                        throw new Error(err.number, getError(err.number));
                    }

                    resultIndex++;
                    if (resultIndex < results.length) {
                        processOneResult();
                    }
                }
            }

            function handleUpdate(operation, doc, isCreate) {
                var old_id = JSON.stringify(doc._id);

                try {
                    switch (operation.type) {
                        // Field Operations
                        case 'Inc':
                            fieldInc(doc, operation.field, operation.value);
                            break;
                        case 'Mul':
                            fieldMul(doc, operation.field, operation.value);
                            break;
                        case 'Min':
                            fieldMin(doc, operation.field, operation.value);
                            break;
                        case 'Max':
                            fieldMax(doc, operation.field, operation.value);
                            break;
                        case 'Rename':
                            fieldRename(doc, operation.field, operation.value);
                            break;
                        case 'Set':
                            fieldSet(doc, operation.field, operation.value);
                            break;
                        case 'Unset':
                            fieldUnset(doc, operation.field);
                            break;
                        case 'CurrentDate':
                            fieldCurrentDate(doc, operation.field, operation.value);
                            break;
                        case 'Bit':
                            fieldBit(doc, operation.field, operation.value, operation.operator);
                            break;

                            // Array Operations
                        case 'AddToSet':
                            arrayAddToSet(doc, operation.field, operation.value);
                            break;
                        case 'Pop':
                            arrayPop(doc, operation.field, operation.value);
                            break;
                        case 'PullAll':
                            arrayPullAll(doc, operation.field, operation.value);
                            break;
                        case 'Pull':
                            arrayPull(doc, operation.field, operation.value);
                            break;
                        case 'Push':
                            arrayPush(doc, operation.field, operation.value, operation.slice, operation.position);
                            break;

                            // Full replace
                        case 'Replace':
                            doc = fullReplace(doc, operation.value);
                            break;

                        case 'SetOnInsert':
                            if (updateSpec.upsert === true && isCreate) {
                                fieldSet(doc, operation.field, operation.value);
                            }
                            break;

                        default:
                            throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidUpdateOperationType, operation.type));
                    }
                } catch (err) {
                    if (err.number == ChakraErrorCodes.JSERR_CantAssignToReadOnly) {
                        //  This can only be hit due to strict mode, as we don't explicitly use read-only properties for DML.
                        //  Convert to graceful error, to correspond to original behavior.
                        // Scenario (incorrect usage): insert({id:1, n:"foo"}); update({ id: 1 }, { $set: { 'n.x': 2 } });
                        // This error is handled in Gateway and converted to a write error.
                        throw new Error(ErrorCodes.BadRequest, "CannotUsePartOfPropertyToTraverseElement");
                    }
                    throw err;
                }

                if (old_id !== JSON.stringify(doc._id) && !isCreate) {
                    throw new Error(ErrorCodes.BadRequest, "CannotUpdateImmutableField");
                }
                return doc;
            }

            // --------------------------------------
            // Field Operations
            // --------------------------------------

            // Operation: $inc
            //   Increments the value of the field by the specified amount.
            function fieldInc(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(typeof(value) == 'number')

                fieldSet(doc, field, value, function (oldValue) {
                    if (oldValue === undefined)
                        return value;
                    if ((typeof oldValue) == "number")
                        return oldValue + value;
                    return oldValue;
                });
            }

            // Operation: $mul
            //   Multiplies the value of the field by the specified amount.
            function fieldMul(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(typeof(value) == 'number')

                fieldSet(doc, field, value, function (oldValue) {
                    if (oldValue === undefined)
                        return 0;
                    if ((typeof oldValue) == "number")
                        return oldValue * value;
                    return oldValue;
                });
            }

            // Operation: $min
            //   Only updates the field if the specified value is less than the existing field value.
            function fieldMin(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(value !== undefined)

                fieldSet(doc, field, value, function (oldValue) {
                    if (oldValue === undefined)
                        return value;
                    return compare(value, oldValue) < 0 ? value : oldValue;
                });
            }

            // Operation: $max
            //   Only updates the field if the specified value is greater than the existing field value.
            function fieldMax(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(value !== undefined)

                fieldSet(doc, field, value, function (oldValue) {
                    if (oldValue === undefined)
                        return value;
                    return compare(value, oldValue) > 0 ? value : oldValue;
                });
            }

            // Operation: $rename
            //   Renames a field.
            function fieldRename(doc, field, newField) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires((typeof newField+ == 'string')
                // Requires(newField+ != '')

                var value = fieldUnset(doc, field);
                if (value !== undefined) {
                    fieldSet(doc, newField, value);
                }

                return value;
            }

            // Operation: $set
            //    Sets the value of a field in a document.
            function fieldSet(doc, field, value, applyOp) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(value !== undefined)
                // Requires((applyOp === undefined) || (typeof(applyOp) == 'function'))

                var navResult = navigateTo(doc, field, true);

                doc = navResult[0];
                field = navResult[1];

                doc[field] = applyOp ? applyOp(doc[field]) : value;
            }

            // Operation: $unset
            //   Removes the specified field from a document.
            function fieldUnset(doc, field) {
                // Requires(doc != undefined)
                // Requires(field != undefined)

                var segments = field.split('.');
                var value;

                for (var i = 0; i < segments.length; i++) {
                    if (doc === undefined) break;

                    var seg = segments[i];

                    // if this is the last segment then we delete the field
                    if (i == (segments.length - 1)) {
                        value = doc[seg];
                        delete doc[seg];
                    } else {
                        // Advance to the next segment
                        doc = doc[seg];
                    }
                }

                return value;
            }

            // Operation: $currentDate
            //   Sets the value of a field to current date, either as a Date or a Timestamp.
            function fieldCurrentDate(doc, field, type) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires((type == 'date') || (type == 'timestamp'))

                var value = type === 'date' ?
					{ $date: new Date().toISOString() } :
                    { $timestamp: { t: Math.floor(new Date().getTime() / 1000), i: 1 } };

                fieldSet(doc, field, value);
            }

            // Operation: $bit
            //   Performs bitwise AND, OR, and XOR updates of integer values.
            function fieldBit(doc, field, value, op) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(typeof(value) == 'number')
                // Requires((op == 'And') || (op == 'Or') || (op == 'Xor'))

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if (doc && ((typeof doc[field]) == 'number')) {
                    switch (op) {
                        case 'And':
                            doc[field] &= value;
                            break;
                        case 'Or':
                            doc[field] |= value;
                            break;
                        case 'Xor':
                            doc[field] ^= value;
                            break;
                    }
                }
            }

            // --------------------------------------
            // Array Operations
            // --------------------------------------

            // Operation: $addToSet
            //  Adds elements to an array only if they do not already exist in the set.
            function arrayAddToSet(doc, field, values) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(Array.isArray(values))

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if (doc[field] === undefined) {
                    doc[field] = new Array();
                } else {
                    if (!Array.isArray(doc[field]))
                        throw new Error(ErrorCodes.BadRequest, 'AddToSet operation requires a target array field.');
                }

                for (var i = 0; i < values.length; i++) {
                    if (!arrayContains(doc[field], values[i])) {
                        doc[field].push(values[i]);
                    }
                }
            }

            // Operation: $pop
            //  Removes the first or last item of an array.
            function arrayPop(doc, field, firstLast) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(firstLast == -1 OR firstLast == 1)

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc != undefined) && Array.isArray(doc[field])) {
                    if (firstLast == -1) {
                        doc[field].shift();
                    } else if (firstLast == 1) {
                        doc[field].pop();
                    }
                }
            }

            // Operation: $pullAll
            //  Removes all matching values from an array.
            function arrayPullAll(doc, field, values) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(Array.isArray(values))

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc != undefined) && Array.isArray(doc[field])) {
                    var array = doc[field];
                    var result = [];

                    for (var i = 0; i < array.length; i++) {
                        if (!arrayContains(values, array[i])) {
                            result.push(array[i]);
                        }
                    }

                    doc[field] = result;
                }
            }

            // Operation: $pull
            //  Removes all array elements that match a specified query.
            function arrayPull(doc, field, value) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(filter != undefined)

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc != undefined) && Array.isArray(doc[field])) {
                    var array = doc[field];
                    var result = [];

                    for (var i = 0; i < array.length; i++) {
                        if (!arrayPullFilter(array[i], value)) {
                            result.push(array[i]);
                        }
                    }

                    doc[field] = result;
                }
            }

            // Operation: $push
            //  Adds an item to an array.
            function arrayPush(doc, field, values, sliceModifier, positionModifier) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(Array.isArray(values))

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if (doc[field] === undefined) {
                    doc[field] = values;
                }
                else {
                    if (!Array.isArray(doc[field]))
                        throw new Error(ErrorCodes.BadRequest, InternalErrors.PushOperatorRequiresTargetArray);

                    if (positionModifier !== undefined) {
                        for (var i = 0; i < values.length; i++) {
                            doc[field].splice(positionModifier, 0, values[i]);
                            positionModifier++;
                        }
                    } else {
                        for (var i = 0; i < values.length; i++) {
                            doc[field].push(values[i]);
                        }
                    }
                }

                if (sliceModifier !== undefined) {
                    if (sliceModifier < 0) {
                        doc[field] = doc[field].slice(sliceModifier);
                    } else {
                        doc[field] = doc[field].slice(0, sliceModifier);
                    }
                }
            }

            function fullReplace(doc, value) {
                var temp = doc;
                if (updateOptions.isParseSystemCollection === true) {
                    if (value.hasOwnProperty("_id") && (JSON.stringify(doc.value._id) !== JSON.stringify(value._id))) {
                        throw new Error(ErrorCodes.BadRequest, "CannotReplaceImmutableField");
                    }

                    doc.value = value;
                    doc.value.id = temp.value.id;
                    doc.value._id = temp.value._id;
                }
                else {
                    if (value.hasOwnProperty("_id") && (JSON.stringify(doc._id) !== JSON.stringify(value._id))) {
                        throw new Error(ErrorCodes.BadRequest, "CannotReplaceImmutableField");
                    }

                    doc = value;
                    doc._id = temp._id;
                }

                doc.id = temp.id;
                doc._self = temp._self;
                doc._etag = temp._etag;

                return doc;
            }


            // --------------------------------------
            // Common Utility Functions
            // --------------------------------------
            function navigateTo(doc, path, upsert) {
                // Requires(doc !== undefined)
                // Requires(path !== undefined)

                var segments = path.split('.');

                var seg;
                for (var i = 0; i < segments.length; i++) {
                    if (doc === undefined) break;

                    seg = segments[i];

                    // We stop at the last path segment and return its value
                    if (i == (segments.length - 1)) break;

                    // If upsert is set and the segment does not exist then create it
                    if (upsert && (doc[seg] === undefined)) {
                        doc[seg] = {};
                    }

                    // Advance to the next segment
                    doc = doc[seg];
                }

                return [doc, seg];
            }

            function arrayContains(array, item) {
                // Requires(Array.isArray(array))
                // Requires(item !=== undefined)

                if ((typeof item) == 'object') {
                    for (var i = 0; i < array.length; i++) {
                        if (compare(array[i], item) == 0)
                            return true;
                    }

                    return false;
                }

                return array.indexOf(item) >= 0;
            }

            function objectsEquivalent(left, right, depth) {
                if (Object.getPrototypeOf(left) !== Object.getPrototypeOf(right)) return false;

                var leftPropertyNames = Object.getOwnPropertyNames(left);
                var rightPropertyNames = Object.getOwnPropertyNames(right);

                if (leftPropertyNames.length != rightPropertyNames.length) {
                    return false;
                }

                for (var i = 0; i < leftPropertyNames.length; i++) {
                    var leftProp = leftPropertyNames[i];
                    var rightProp = rightPropertyNames[i];

                    // Mongo behavior: {a: 1, b: 2} != {b: 2, a: 1}
                    if (leftProp !== rightProp) {
                        return false;
                    }

                    if (typeof (left[leftProp]) == 'object') {
                        if (compare(left[leftProp], right[leftProp], depth + 1) != 0) {
                            return false;
                        }
                    } else {
                        if (left[leftProp] !== right[leftProp]) {
                            return false;
                        }
                    }
                }
                return true;
            }

            function arraysEquivalent(left, right, depth) {
                if (left === right) return true;
                if (left === null || right === null) return false;
                if (left.length != right.length) return false;

                if (Object.getOwnPropertyNames(left).length > left.length + 1 || Object.getOwnPropertyNames(right).length > right.length + 1) {
                    return objectsEquivalent(left, right, depth);
                }

                for (var i = 0; i < left.length; i++) {
                    if (compare(left[i], right[i], depth + 1) != 0) {
                        return false;
                    }
                }
                return true;
            }

            function compare(value1, value2, depth) {
                // Requires(value1 !== undefined)
                // Requires(value2 !== undefined)

                // To prevent infinite object property reference loop
                if (depth === undefined) depth = 1;
                if (depth > 1000) return false;

                var t1 = getTypeOrder(value1);
                var t2 = getTypeOrder(value2);

                if (t1 === t2) {
                    if (value1 == value2) return 0;

                    if (Array.isArray(value1)) {
                        if (arraysEquivalent(value1, value2, depth)) return 0;
                    }
                    else if (typeof value1 == 'object') {
                        if (objectsEquivalent(value1, value2, depth)) return 0;
                    } else {
                        return (value1 < value2) ? -1 : 1;
                    }

                    return (value1 < value2) ? -1 : 1;
                }

                return t1 < t2 ? -1 : 1;
            }

            // If the specified <value> to remove is an array, $pull removes only the elements in the array that match the
            // specified <value> exactly, including order. If the specified <value> to remove is a document, $pull removes only
            // the elements in the array that have/[contain] the exact same fields and values. The ordering of the fields can differ.
            // Note: Mongo ignores the pull operator when the item to be removed (pullItem) is [] or {}.
            let arrayPullFilter = function (arrayItem, pullItem) {
                if (typeof pullItem == 'object' && pullItem !== null) {
                    if (typeof arrayItem == 'object' && arrayItem !== null) {
                        if (Array.isArray(arrayItem)) { // Array [] case
                            if (Array.isArray(pullItem) && (pullItem.length === arrayItem.length)) {
                                for (var i = 0; i < arrayItem.length; i++) {
                                    if (typeof pullItem[i] == 'object') {
                                        if (!arrayPullFilter(arrayItem[i], pullItem[i])) {
                                            return false;
                                        }
                                    } else {
                                        if (pullItem[i] !== arrayItem[i]) {
                                            return false;
                                        }
                                    }
                                }
                                return true;
                            } else {
                                return false;
                            }
                            return true;
                        } else { // Object {} case
                            if (Array.isArray(pullItem)) return false;
                            if (Object.keys(pullItem).length == 0) return true;

                            for (var p in pullItem) {
                                if (typeof pullItem[p] == 'object' && arrayItem.hasOwnProperty(p)) {
                                    if (!arrayPullFilter(arrayItem[p], pullItem[p])) {
                                        return false;
                                    }
                                } else {
                                    if (!arrayItem.hasOwnProperty(p) || !(arrayItem[p] === pullItem[p])) {
                                        return false;
                                    }
                                }
                            }
                            return true;
                        }
                    } else {
                        return false;
                    }
                }
                else {
                    return arrayItem === pullItem;
                }
            }

            function getTypeOrder(value) {
                // Requires(value !== undefined)

                // Here is the type ordering 
                // 1.MinKey (internal type)
                // 2.Null
                // 3.Numbers (ints, longs, doubles)
                // 4.Symbol, String
                // 5.Object
                // 6.Array
                // 7.BinData
                // 8.ObjectId
                // 9.Boolean
                // 10.Date, Timestamp
                // 11.Regular Expression
                // 12.MaxKey (internal type)

                switch (typeof value) {
                    case "number":
                        return 3;
                    case "string":
                    case "symbol":
                        return 4;
                    case "boolean":
                        return 9;
                    case "object":
                        if (value === null) return 2;
                        if (Array.isArray(value)) return 6;
                        return 5;
                    default:
                        return 12;
                }
            }

            function setResponse(count, err) {
                __.response.setBody({
                    count: count,
                    errorCode: err ? err : 0
                });
            }

            function sprintf(format) {
                var args = arguments;
                var i = 1;
                return format.replace(/%((%)|s)/g, function (matchStr, subMatch1, subMatch2) {
                    // In case of %% subMatch2 would be '%'.
                    return subMatch2 || args[i++];
                });
            }
        }