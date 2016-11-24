export function containsClauses(obj: any): boolean {
  if (obj === null || typeof obj !== "object") {
    return false;
  }

  if (Array.isArray(obj)) { 
    return false;
  }

  for (let key of Object.keys(obj)) {
    if (key.startsWith('$') || containsClauses(obj[key])) {
      return true;
    }
  }
  return false;
}
/**
 Remove $push, $pop, $in etc.
 */
export function filterClauses(obj: any): any {
  const copy:any = {};

  if (obj === null || typeof obj !== "object") {
    return obj;
  }

  if (Array.isArray(obj)){
    return obj;
  }

  for (let key of Object.keys(obj)) {
    if (!key.startsWith('$')) {
      copy[key] = filterClauses(obj[key]);
    }
  }
  return copy;
}
