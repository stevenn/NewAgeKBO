import type { EnterpriseDetail } from '@/app/api/enterprises/[number]/route'

export type ChangeType = 'added' | 'removed' | 'changed' | 'unchanged'

export interface FieldChange {
  type: ChangeType
  oldValue?: string | null
  newValue?: string | null
}

export interface EnterpriseComparison {
  status: FieldChange
  juridicalForm: FieldChange
  juridicalSituation: FieldChange
  typeOfEnterprise: FieldChange
  startDate: FieldChange
  denominations: {
    added: typeof EnterpriseDetail.prototype.denominations
    removed: typeof EnterpriseDetail.prototype.denominations
    unchanged: typeof EnterpriseDetail.prototype.denominations
  }
  addresses: {
    added: typeof EnterpriseDetail.prototype.addresses
    removed: typeof EnterpriseDetail.prototype.addresses
    unchanged: typeof EnterpriseDetail.prototype.addresses
  }
  activities: {
    added: typeof EnterpriseDetail.prototype.activities
    removed: typeof EnterpriseDetail.prototype.activities
    unchanged: typeof EnterpriseDetail.prototype.activities
  }
  contacts: {
    added: typeof EnterpriseDetail.prototype.contacts
    removed: typeof EnterpriseDetail.prototype.contacts
    unchanged: typeof EnterpriseDetail.prototype.contacts
  }
  establishments: {
    added: typeof EnterpriseDetail.prototype.establishments
    removed: typeof EnterpriseDetail.prototype.establishments
    unchanged: typeof EnterpriseDetail.prototype.establishments
  }
}

function compareField(oldValue: string | null | undefined, newValue: string | null | undefined): FieldChange {
  if (oldValue === newValue || (! oldValue && !newValue)) {
    return { type: 'unchanged', oldValue: oldValue ?? null, newValue: newValue ?? null }
  }
  if (!oldValue) {
    return { type: 'added', newValue: newValue ?? null }
  }
  if (!newValue) {
    return { type: 'removed', oldValue: oldValue ?? null }
  }
  return { type: 'changed', oldValue, newValue }
}

export function compareEnterprises(
  current: EnterpriseDetail,
  previous: EnterpriseDetail | null
): EnterpriseComparison | null {
  if (!previous) return null

  // Compare basic fields
  const comparison: EnterpriseComparison = {
    status: compareField(previous.status, current.status),
    juridicalForm: compareField(previous.juridicalForm, current.juridicalForm),
    juridicalSituation: compareField(previous.juridicalSituation, current.juridicalSituation),
    typeOfEnterprise: compareField(previous.typeOfEnterprise, current.typeOfEnterprise),
    startDate: compareField(previous.startDate, current.startDate),
    denominations: { added: [], removed: [], unchanged: [] },
    addresses: { added: [], removed: [], unchanged: [] },
    activities: { added: [], removed: [], unchanged: [] },
    contacts: { added: [], removed: [], unchanged: [] },
    establishments: { added: [], removed: [], unchanged: [] },
  }

  // Compare denominations
  current.denominations.forEach((denom) => {
    const exists = previous.denominations.find(
      (p) =>
        p.language === denom.language &&
        p.typeCode === denom.typeCode &&
        p.denomination === denom.denomination
    )
    if (exists) {
      comparison.denominations.unchanged.push(denom)
    } else {
      comparison.denominations.added.push(denom)
    }
  })
  previous.denominations.forEach((denom) => {
    const exists = current.denominations.find(
      (c) =>
        c.language === denom.language &&
        c.typeCode === denom.typeCode &&
        c.denomination === denom.denomination
    )
    if (!exists) {
      comparison.denominations.removed.push(denom)
    }
  })

  // Compare addresses
  current.addresses.forEach((addr) => {
    const key = `${addr.typeCode}-${addr.streetNL}-${addr.zipcode}`
    const exists = previous.addresses.find(
      (p) => `${p.typeCode}-${p.streetNL}-${p.zipcode}` === key
    )
    if (exists) {
      comparison.addresses.unchanged.push(addr)
    } else {
      comparison.addresses.added.push(addr)
    }
  })
  previous.addresses.forEach((addr) => {
    const key = `${addr.typeCode}-${addr.streetNL}-${addr.zipcode}`
    const exists = current.addresses.find(
      (c) => `${c.typeCode}-${c.streetNL}-${c.zipcode}` === key
    )
    if (!exists) {
      comparison.addresses.removed.push(addr)
    }
  })

  // Compare activities
  current.activities.forEach((act) => {
    const key = `${act.naceVersion}-${act.naceCode}-${act.classification}`
    const exists = previous.activities.find(
      (p) => `${p.naceVersion}-${p.naceCode}-${p.classification}` === key
    )
    if (exists) {
      comparison.activities.unchanged.push(act)
    } else {
      comparison.activities.added.push(act)
    }
  })
  previous.activities.forEach((act) => {
    const key = `${act.naceVersion}-${act.naceCode}-${act.classification}`
    const exists = current.activities.find(
      (c) => `${c.naceVersion}-${c.naceCode}-${c.classification}` === key
    )
    if (!exists) {
      comparison.activities.removed.push(act)
    }
  })

  // Compare contacts
  current.contacts.forEach((contact) => {
    const key = `${contact.contactType}-${contact.value}`
    const exists = previous.contacts.find((p) => `${p.contactType}-${p.value}` === key)
    if (exists) {
      comparison.contacts.unchanged.push(contact)
    } else {
      comparison.contacts.added.push(contact)
    }
  })
  previous.contacts.forEach((contact) => {
    const key = `${contact.contactType}-${contact.value}`
    const exists = current.contacts.find((c) => `${c.contactType}-${c.value}` === key)
    if (!exists) {
      comparison.contacts.removed.push(contact)
    }
  })

  // Compare establishments
  current.establishments.forEach((est) => {
    const exists = previous.establishments.find(
      (p) => p.establishmentNumber === est.establishmentNumber
    )
    if (exists) {
      comparison.establishments.unchanged.push(est)
    } else {
      comparison.establishments.added.push(est)
    }
  })
  previous.establishments.forEach((est) => {
    const exists = current.establishments.find(
      (c) => c.establishmentNumber === est.establishmentNumber
    )
    if (!exists) {
      comparison.establishments.removed.push(est)
    }
  })

  return comparison
}
