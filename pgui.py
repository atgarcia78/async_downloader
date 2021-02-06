


from tkinter import Tk, Text, END
root = Tk()
text = Text(root)
text.pack()
for i, dl in enumerate(list_dl):
    line = i * 2
    text.insert(f"{line}.1", dl.webpage_url)

    for line in p.stdout:
        text.insert(END, line)
root.mainloop()