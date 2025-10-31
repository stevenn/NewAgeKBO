'use client'

import { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import type { Language } from '@/lib/types/codes'

interface LanguageContextType {
  language: Language
  setLanguage: (lang: Language) => void
  isInitialized: boolean
}

const LanguageContext = createContext<LanguageContextType | undefined>(undefined)

const LANGUAGE_STORAGE_KEY = 'kbo-language-preference'

export function LanguageProvider({ children }: { children: ReactNode }) {
  // Default to NL, will be updated from localStorage on mount
  const [language, setLanguageState] = useState<Language>('NL')
  const [isInitialized, setIsInitialized] = useState(false)

  // Load language preference from localStorage on mount
  useEffect(() => {
    const stored = localStorage.getItem(LANGUAGE_STORAGE_KEY) as Language | null
    if (stored && ['NL', 'FR', 'DE'].includes(stored)) {
      setLanguageState(stored)
    }
    setIsInitialized(true)
  }, [])

  // Wrapper to persist language changes
  const setLanguage = (lang: Language) => {
    setLanguageState(lang)
    localStorage.setItem(LANGUAGE_STORAGE_KEY, lang)
  }

  return (
    <LanguageContext.Provider value={{ language, setLanguage, isInitialized }}>
      {children}
    </LanguageContext.Provider>
  )
}

export function useLanguage() {
  const context = useContext(LanguageContext)
  if (context === undefined) {
    throw new Error('useLanguage must be used within a LanguageProvider')
  }
  return context
}
