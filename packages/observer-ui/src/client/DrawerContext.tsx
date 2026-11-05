import { type ReactNode, createContext, useContext, useEffect, useRef, useState } from "react"
import { useLocation } from "react-router"

import { X } from "lucide-react"

import { Button } from "../components/ui/button.js"
import { Drawer, DrawerClose, DrawerContent } from "../components/ui/drawer.js"

export type DrawerEntry = { key: string; content: ReactNode }

type DrawerState = { open: true; entry: DrawerEntry } | { open: false; entry: DrawerEntry | null }

type DrawerContextType = {
    openKey: string | null
    show: (entry: DrawerEntry, trigger: HTMLElement) => void
    close: () => void
}

const DrawerContext = createContext<DrawerContextType | undefined>(undefined)

export function DrawerProvider({ children }: { children: ReactNode }) {
    const [state, setState] = useState<DrawerState>({ open: false, entry: null })
    const trigger = useRef<HTMLElement | null>(null)
    const { pathname } = useLocation()
    function show(entry: DrawerEntry, from: HTMLElement) {
        trigger.current = from
        setState({ open: true, entry })
    }
    function close() {
        setState(({ entry }) => ({ open: false, entry }))
    }
    useEffect(close, [pathname])
    return (
        <DrawerContext.Provider value={{ openKey: state.open ? state.entry.key : null, show, close }}>
            {children}
            <Drawer direction="right" autoFocus open={state.open} onOpenChange={open => !open && close()}>
                <DrawerContent
                    className="bg-card font-satoshi text-card-foreground antialiased shadow-(--shadow-dialog) data-[vaul-drawer-direction=right]:w-full"
                    onCloseAutoFocus={event => {
                        event.preventDefault()
                        trigger.current?.focus()
                    }}
                >
                    {state.entry?.content}
                    <DrawerClose asChild>
                        <Button className="absolute top-3 right-3" variant="ghost" size="icon" aria-label="Close">
                            <X aria-hidden />
                        </Button>
                    </DrawerClose>
                </DrawerContent>
            </Drawer>
        </DrawerContext.Provider>
    )
}

export function useDrawer(): DrawerContextType {
    const context = useContext(DrawerContext)
    if (!context) throw new Error("useDrawer must be used within a DrawerProvider")
    return context
}
