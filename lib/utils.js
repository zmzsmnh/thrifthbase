module.exports.mergeDict = function(baseDict, newDict) {
	newDict = typeof newDict !== 'undefined' ? newDict : {};
    var keys = Object.keys(baseDict);
    keys.forEach(function(k) {
        if (k in newDict) {
            newDict[k] = mergeDict(baseDict[k]);
        } else {
            newDict[k] = baseDict[k];
        }
    });
    return newDict;
};